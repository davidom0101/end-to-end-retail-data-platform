"""Process published S3 deliveries and build the Redshift reporting mart."""

import json
import os
import subprocess
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import TypedDict, cast
from uuid import uuid4

import boto3
from prefect import flow, get_run_logger, task
from prefect.client.schemas.objects import State
from prefect.states import get_state_exception

from retail_pipeline.audit import AuditBuffer, BatchKey, audit_stage, run_audit
from retail_pipeline.classification import CLASSIFICATION_DECISIONS
from retail_pipeline.discovery import (
    PublishedDelivery,
    SnapshotSource,
    VerifiedDeliverySource,
    discover_deliveries,
    manifest_uri_batch_key,
    read_published_delivery,
    verify_delivery_files,
)
from retail_pipeline.staging import (
    AppliedBatch,
    AppliedDelivery,
    LoadedSnapshot,
    RedshiftClient,
    RetryableWarehouseError,
    SnapshotStagingError,
    WarehouseWaitTimeout,
    apply_incremental,
    classify_incremental,
    commit_snapshot_load,
    read_applied_batches,
    read_published_snapshot,
    stage_incremental,
    stage_snapshot_for_load,
)
from retail_pipeline.transient import is_transient_aws_error


@dataclass(frozen=True)
class DeliveryRun:
    dataset_id: str
    batch_id: str
    status: str
    counts: dict[str, dict[str, int]]


class FailurePoint(StrEnum):
    BEFORE_CORE = "before-core"
    BEFORE_COMMIT = "before-commit"
    AFTER_COMMIT = "after-commit"


def _retry_transient_failure(_task: object, _run: object, state: State) -> bool:
    error = cast(BaseException, get_state_exception(state))
    return is_transient_aws_error(error) or isinstance(
        error, (RetryableWarehouseError, WarehouseWaitTimeout)
    )


class _RetryOptions(TypedDict):
    retries: int
    retry_delay_seconds: list[float]
    retry_condition_fn: Callable[[object, object, State], bool]


_RETRY_OPTIONS: _RetryOptions = {
    "retries": 2,
    "retry_delay_seconds": [2, 5],
    "retry_condition_fn": _retry_transient_failure,
}


def _batch_key(published: PublishedDelivery) -> tuple[str, str]:
    return published.delivery.dataset_id, published.delivery.batch_id


@task(name="read-published-manifest", persist_result=False, **_RETRY_OPTIONS)
def _read_manifest(
    s3: SnapshotSource,
    source_manifest_uri: str,
    *,
    historical_only: bool = False,
    audit: AuditBuffer | None = None,
) -> PublishedDelivery:
    batch_key = manifest_uri_batch_key(source_manifest_uri) or (None, None)
    with audit_stage(audit, "read_manifest", (batch_key,)) as outcome:
        if historical_only:
            published = read_published_snapshot(s3, source_manifest_uri)
        else:
            published = read_published_delivery(s3, source_manifest_uri)
        outcome.batch_keys = (_batch_key(published),)
    get_run_logger().info(
        "Read dataset %s, batch %s from %s",
        published.delivery.dataset_id,
        published.delivery.batch_id,
        source_manifest_uri,
    )
    return published


@task(name="stage-incremental", persist_result=False, **_RETRY_OPTIONS)
def _stage_incremental(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    published: PublishedDelivery,
    *,
    workgroup: str,
    database: str,
) -> dict[str, int]:
    counts = stage_incremental(
        s3, redshift, published, workgroup=workgroup, database=database
    )
    get_run_logger().info("Verified incremental staging row counts: %s", counts)
    return counts


_classify_incremental = task(
    name="classify-incremental-delivery",
    persist_result=False,
    **_RETRY_OPTIONS,
)(classify_incremental)


@task(name="read-applied-batches", persist_result=False, **_RETRY_OPTIONS)
def _read_ledger(
    redshift: RedshiftClient,
    *,
    workgroup: str,
    database: str,
    core_schema: str,
    audit: AuditBuffer | None = None,
    batch_keys: tuple[BatchKey, ...] = ((None, None),),
) -> dict[tuple[str, str], AppliedBatch]:
    with audit_stage(audit, "read_ledger", batch_keys):
        return read_applied_batches(
            redshift, workgroup=workgroup, database=database, core_schema=core_schema
        )


@task(name="discover-published-deliveries", persist_result=False, **_RETRY_OPTIONS)
def _discover(
    s3: VerifiedDeliverySource,
    dataset_id: str,
    through: date,
    *,
    audit: AuditBuffer | None = None,
) -> list[PublishedDelivery]:
    with audit_stage(audit, "discover_deliveries", ((dataset_id, None),)):
        deliveries = discover_deliveries(
            s3,
            bucket=_required_env("RETAIL_RAW_BUCKET"),
            dataset_id=dataset_id,
            cutoff=through,
        )
    get_run_logger().info(
        "Discovered %s deliveries for %s", len(deliveries), dataset_id
    )
    return deliveries


@task(name="verify-published-delivery", persist_result=False, **_RETRY_OPTIONS)
def _verified_replay(
    s3: VerifiedDeliverySource,
    published: PublishedDelivery,
    recorded: AppliedBatch | None,
    *,
    audit: AuditBuffer | None = None,
) -> bool:
    with audit_stage(audit, "verify_delivery", (_batch_key(published),)) as outcome:
        delivery = published.delivery
        if recorded and (
            recorded.kind != delivery.kind
            or recorded.manifest_sha256 != published.manifest_sha256
        ):
            raise SnapshotStagingError(
                f"Applied batch {delivery.dataset_id}/{delivery.batch_id} "
                "has a different manifest"
            )
        verify_delivery_files(s3, published)
        counts: dict[str, int] = {}
        for file in delivery.files:
            counts[file.entity_type] = counts.get(file.entity_type, 0) + file.row_count
        outcome.row_counts = counts
    return recorded is not None


@task(name="apply-incremental-delivery", persist_result=False, **_RETRY_OPTIONS)
def _apply_incremental(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    published: PublishedDelivery,
    *,
    workgroup: str,
    database: str,
    core_schema: str,
    fail_before_commit: bool = False,
    audit: AuditBuffer | None = None,
) -> AppliedDelivery:
    with audit_stage(audit, "core_application", (_batch_key(published),)) as outcome:
        applied = apply_incremental(
            s3,
            redshift,
            published,
            workgroup=workgroup,
            database=database,
            core_schema=core_schema,
            fail_before_commit=fail_before_commit,
        )
        outcome.status = "committed" if applied.status == "applied" else applied.status
        outcome.row_counts = applied.counts
        return applied


@task(name="build-and-publish-mart", persist_result=False)
def _build_mart(
    core_schema: str,
    project_dir: Path,
    audit: AuditBuffer | None = None,
    batch_keys: tuple[BatchKey, ...] = ((None, None),),
) -> None:
    variables = {
        "core_schema": core_schema,
        "sales_candidate_suffix": f"_{uuid4().hex}",
    }
    with audit_stage(audit, "dbt_build", batch_keys):
        _run_dbt(["build"], project_dir, variables)
    with audit_stage(audit, "reporting_publication", batch_keys) as outcome:
        _run_dbt(["run-operation", "publish_daily_sales"], project_dir, variables)
        outcome.status = "published"
    get_run_logger().info("Published the validated mart_daily_sales view")


def _run_dbt(
    arguments: list[str], project_dir: Path, variables: dict[str, str]
) -> None:
    command = [
        "dbt",
        "--no-use-colors",
        *arguments,
        "--project-dir",
        str(project_dir),
        "--vars",
        json.dumps(variables),
    ]
    logger = get_run_logger()
    with subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        errors="replace",
    ) as process:
        assert process.stdout is not None
        for line in process.stdout:
            if line.strip():
                logger.info("%s", line.rstrip())
        exit_code = process.wait()
    if exit_code:
        raise RuntimeError(
            f"dbt {' '.join(arguments)} failed with exit code {exit_code}"
        )


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Set {name} on the pipeline runner")
    return value


@contextmanager
def _runner_clients() -> Iterator[tuple[VerifiedDeliverySource, RedshiftClient]]:
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not region:
        raise ValueError("Set AWS_REGION or AWS_DEFAULT_REGION on the pipeline runner")
    session = boto3.Session(
        profile_name=os.environ.get("AWS_PROFILE"), region_name=region
    )
    with (
        closing(session.client("s3")) as s3_client,
        closing(session.client("redshift-data")) as redshift_client,
    ):
        yield (
            cast(VerifiedDeliverySource, s3_client),
            cast(RedshiftClient, redshift_client),
        )


def _dbt_project() -> Path:
    project = Path("dbt").resolve()
    if not (project / "dbt_project.yml").is_file():
        raise FileNotFoundError("Run the pipeline from the repository root")
    return project


@task(name="apply-historical-snapshot", persist_result=False, **_RETRY_OPTIONS)
def _apply_snapshot(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    published: PublishedDelivery,
    *,
    workgroup: str,
    database: str,
    core_schema: str,
    audit: AuditBuffer | None = None,
) -> LoadedSnapshot:
    with audit_stage(audit, "core_application", (_batch_key(published),)) as outcome:
        staged = stage_snapshot_for_load(
            s3,
            redshift,
            published,
            workgroup=workgroup,
            database=database,
            core_schema=core_schema,
        )
        logger = get_run_logger()
        logger.info("Verified staged row counts: %s", staged.snapshot.row_counts)
        outcome.row_counts = staged.snapshot.row_counts
        loaded = commit_snapshot_load(redshift, staged)
        outcome.status = "committed" if loaded.status == "applied" else loaded.status
    logger.info(
        "Snapshot %s in %s: %s", loaded.status, loaded.core_schema, loaded.row_counts
    )
    return loaded


@flow(name="historical-snapshot", persist_result=False)
def run_snapshot(source_manifest_uri: str) -> LoadedSnapshot:
    """Stage, load, and build the mart for one published historical snapshot."""
    workgroup = _required_env("RETAIL_REDSHIFT_WORKGROUP")
    database = _required_env("RETAIL_REDSHIFT_DATABASE")
    core_schema = _required_env("RETAIL_CORE_SCHEMA")
    project_dir = _dbt_project()

    with (
        _runner_clients() as (s3, redshift),
        run_audit(
            redshift, workgroup=workgroup, database=database, core_schema=core_schema
        ) as audit,
    ):
        published = _read_manifest(
            s3, source_manifest_uri, historical_only=True, audit=audit
        )
        batch_keys = (_batch_key(published),)
        ledger = _read_ledger(
            redshift,
            workgroup=workgroup,
            database=database,
            core_schema=core_schema,
            audit=audit,
            batch_keys=batch_keys,
        )
        delivery = published.delivery
        if _verified_replay(
            s3,
            published,
            ledger.get((delivery.dataset_id, delivery.batch_id)),
            audit=audit,
        ):
            with audit_stage(audit, "core_application", batch_keys) as outcome:
                outcome.status = "already_applied"
                outcome.row_counts = {}
            counts: dict[str, int] = {}
            for file in delivery.files:
                counts[file.entity_type] = (
                    counts.get(file.entity_type, 0) + file.row_count
                )
            loaded = LoadedSnapshot(
                source_manifest_uri, core_schema, counts, "already_applied"
            )
            get_run_logger().info(
                "Verified snapshot replay %s/%s", delivery.dataset_id, delivery.batch_id
            )
        else:
            loaded = _apply_snapshot(
                s3,
                redshift,
                published,
                workgroup=workgroup,
                database=database,
                core_schema=core_schema,
                audit=audit,
            )

        _build_mart(loaded.core_schema, project_dir, audit, batch_keys)
    return loaded


@flow(name="incremental-staging", persist_result=False)
def run_incremental_staging(source_manifest_uri: str) -> dict[str, int]:
    """Stage and verify a published incremental delivery."""
    workgroup = _required_env("RETAIL_REDSHIFT_WORKGROUP")
    database = _required_env("RETAIL_REDSHIFT_DATABASE")
    with _runner_clients() as (s3, redshift):
        published = _read_manifest(s3, source_manifest_uri)
        return _stage_incremental(
            s3, redshift, published, workgroup=workgroup, database=database
        )


@flow(name="incremental-classification", persist_result=False)
def run_incremental_classification(
    source_manifest_uri: str,
) -> dict[str, dict[str, int]]:
    """Preview the decisions for one incremental delivery in an existing demo target."""
    workgroup = _required_env("RETAIL_REDSHIFT_WORKGROUP")
    database = _required_env("RETAIL_REDSHIFT_DATABASE")
    core_schema = _required_env("RETAIL_CORE_SCHEMA")
    with _runner_clients() as (s3, redshift):
        published = _read_manifest(s3, source_manifest_uri)
        return _classify_incremental(
            s3,
            redshift,
            published,
            workgroup=workgroup,
            database=database,
            core_schema=core_schema,
        )


@flow(name="incremental-application", persist_result=False)
def run_incremental_application(source_manifest_uri: str) -> AppliedDelivery:
    """Apply one explicitly selected incremental delivery."""
    workgroup = _required_env("RETAIL_REDSHIFT_WORKGROUP")
    database = _required_env("RETAIL_REDSHIFT_DATABASE")
    core_schema = _required_env("RETAIL_CORE_SCHEMA")
    with (
        _runner_clients() as (s3, redshift),
        run_audit(
            redshift, workgroup=workgroup, database=database, core_schema=core_schema
        ) as audit,
    ):
        published = _read_manifest(s3, source_manifest_uri, audit=audit)
        if published.delivery.kind != "incremental":
            raise SnapshotStagingError("Expected an incremental delivery manifest")
        key = _batch_key(published)
        ledger = _read_ledger(
            redshift,
            workgroup=workgroup,
            database=database,
            core_schema=core_schema,
            audit=audit,
            batch_keys=(key,),
        )
        if _verified_replay(s3, published, ledger.get(key), audit=audit):
            with audit_stage(audit, "core_application", (key,)) as outcome:
                outcome.status = "already_applied"
                outcome.row_counts = {}
                return AppliedDelivery("already_applied", {})
        return _apply_incremental(
            s3,
            redshift,
            published,
            workgroup=workgroup,
            database=database,
            core_schema=core_schema,
            audit=audit,
        )


@flow(name="published-deliveries", persist_result=False)
def run_deliveries(
    source_manifest_uri: str | None = None,
    *,
    dataset_id: str | None = None,
    through: date | None = None,
    inject_failure: FailurePoint | None = None,
) -> list[DeliveryRun]:
    """Process one published delivery or all normal deliveries through a cutoff."""
    if inject_failure is not None and source_manifest_uri is None:
        raise ValueError("Failure injection requires an exact incremental manifest URI")
    if source_manifest_uri is not None:
        if dataset_id is not None or through is not None:
            raise ValueError("Choose an S3 manifest URI or a dataset and cutoff")
    elif not dataset_id or through is None:
        raise ValueError("Discovery requires a dataset and UTC cutoff")

    workgroup = _required_env("RETAIL_REDSHIFT_WORKGROUP")
    database = _required_env("RETAIL_REDSHIFT_DATABASE")
    core_schema = _required_env("RETAIL_CORE_SCHEMA")
    project_dir = _dbt_project()
    runs = []
    with (
        _runner_clients() as (s3, redshift),
        run_audit(
            redshift, workgroup=workgroup, database=database, core_schema=core_schema
        ) as audit,
    ):
        if source_manifest_uri is not None:
            deliveries = [_read_manifest(s3, source_manifest_uri, audit=audit)]
        else:
            assert dataset_id is not None and through is not None
            deliveries = _discover(s3, dataset_id, through, audit=audit)
        if not deliveries:
            return []

        batch_keys = tuple(_batch_key(delivery) for delivery in deliveries)
        ledger = _read_ledger(
            redshift,
            workgroup=workgroup,
            database=database,
            core_schema=core_schema,
            audit=audit,
            batch_keys=batch_keys,
        )
        for published in deliveries:
            delivery = published.delivery
            key = (delivery.dataset_id, delivery.batch_id)
            replay = _verified_replay(s3, published, ledger.get(key), audit=audit)
            if inject_failure is not None:
                if delivery.kind != "incremental" or replay:
                    raise ValueError(
                        "Failure injection requires a pending incremental delivery"
                    )
                get_run_logger().info(
                    "Injecting %s failure for %s/%s", inject_failure, *key
                )
            if inject_failure == FailurePoint.BEFORE_CORE:
                with audit_stage(audit, "before_core", (key,)):
                    raise RuntimeError("Injected failure before core writes")
            if replay:
                with audit_stage(audit, "core_application", (key,)) as outcome:
                    outcome.status = "already_applied"
                    outcome.row_counts = {}
                    result = AppliedDelivery("already_applied", {})
            elif delivery.kind == "historical":
                loaded = _apply_snapshot(
                    s3,
                    redshift,
                    published,
                    workgroup=workgroup,
                    database=database,
                    core_schema=core_schema,
                    audit=audit,
                )
                counts = {}
                if loaded.status == "applied":
                    for entity, count in loaded.row_counts.items():
                        counts[entity] = dict.fromkeys(CLASSIFICATION_DECISIONS, 0)
                        counts[entity]["insert"] = count
                result = AppliedDelivery(loaded.status, counts)
            else:
                result = _apply_incremental(
                    s3,
                    redshift,
                    published,
                    workgroup=workgroup,
                    database=database,
                    core_schema=core_schema,
                    fail_before_commit=inject_failure == FailurePoint.BEFORE_COMMIT,
                    audit=audit,
                )
                if inject_failure is not None and result.status == "already_applied":
                    raise ValueError(
                        "Failure injection requires a pending incremental delivery"
                    )
            get_run_logger().info(
                "Delivery %s/%s %s: %s", *key, result.status, result.counts
            )
            if inject_failure == FailurePoint.AFTER_COMMIT:
                with audit_stage(audit, "after_commit", (key,)):
                    raise RuntimeError("Injected failure after core commit, before dbt")
            runs.append(DeliveryRun(*key, result.status, result.counts))

        _build_mart(core_schema, project_dir, audit, batch_keys)
    return runs
