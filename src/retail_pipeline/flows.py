"""Process published S3 deliveries and build the Redshift reporting mart."""

import json
import os
import subprocess
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import cast

import boto3
from prefect import flow, get_run_logger, task

from retail_pipeline.classification import CLASSIFICATION_DECISIONS
from retail_pipeline.discovery import (
    PublishedDelivery,
    SnapshotSource,
    VerifiedDeliverySource,
    discover_deliveries,
    read_published_delivery,
    verify_delivery_files,
)
from retail_pipeline.manifest import v1_layouts
from retail_pipeline.staging import (
    AppliedBatch,
    AppliedDelivery,
    LoadedSnapshot,
    RedshiftClient,
    SnapshotStagingError,
    StagedSnapshotLoad,
    apply_incremental,
    classify_incremental,
    commit_snapshot_load,
    read_applied_batches,
    read_published_snapshot,
    stage_incremental,
    stage_snapshot_for_load,
)


@dataclass(frozen=True)
class DeliveryRun:
    dataset_id: str
    batch_id: str
    status: str
    counts: dict[str, dict[str, int]]


@task(name="read-published-manifest", persist_result=False)
def _read_manifest(
    s3: SnapshotSource, source_manifest_uri: str, *, historical_only: bool = False
) -> PublishedDelivery:
    if historical_only:
        published = read_published_snapshot(s3, source_manifest_uri)
    else:
        published = read_published_delivery(s3, source_manifest_uri)
    get_run_logger().info(
        "Read dataset %s, batch %s from %s",
        published.delivery.dataset_id,
        published.delivery.batch_id,
        source_manifest_uri,
    )
    return published


@task(name="stage-snapshot", persist_result=False)
def _stage(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    published: PublishedDelivery,
    *,
    workgroup: str,
    database: str,
    core_schema: str,
) -> StagedSnapshotLoad:
    staged = stage_snapshot_for_load(
        s3,
        redshift,
        published,
        workgroup=workgroup,
        database=database,
        core_schema=core_schema,
    )
    get_run_logger().info("Verified staged row counts: %s", staged.snapshot.row_counts)
    return staged


@task(name="stage-incremental", persist_result=False)
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


@task(name="load-core", persist_result=False)
def _load_core(redshift: RedshiftClient, staged: StagedSnapshotLoad) -> LoadedSnapshot:
    loaded = commit_snapshot_load(redshift, staged)
    get_run_logger().info(
        "Snapshot %s in %s: %s", loaded.status, loaded.core_schema, loaded.row_counts
    )
    return loaded


@task(name="discover-published-deliveries", persist_result=False)
def _discover(
    s3: VerifiedDeliverySource, dataset_id: str, through: date
) -> list[PublishedDelivery]:
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


@task(name="verify-published-delivery", persist_result=False)
def _verified_replay(
    s3: VerifiedDeliverySource,
    published: PublishedDelivery,
    recorded: AppliedBatch | None,
) -> bool:
    delivery = published.delivery
    if recorded and (
        recorded.kind != delivery.kind
        or recorded.manifest_sha256 != published.manifest_sha256
    ):
        raise SnapshotStagingError(
            f"Applied batch {delivery.dataset_id}/{delivery.batch_id} "
            "has a different manifest"
        )
    v1_layouts(delivery)
    verify_delivery_files(s3, published)
    return recorded is not None


@task(name="apply-incremental-delivery", persist_result=False)
def _apply_incremental(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    published: PublishedDelivery,
    *,
    workgroup: str,
    database: str,
    core_schema: str,
) -> AppliedDelivery:
    return apply_incremental(
        s3,
        redshift,
        published,
        workgroup=workgroup,
        database=database,
        core_schema=core_schema,
    )


@task(name="dbt-build", persist_result=False)
def _build_mart(core_schema: str, project_dir: Path) -> None:
    command = [
        "dbt",
        "--no-use-colors",
        "build",
        "--project-dir",
        str(project_dir),
        "--vars",
        json.dumps({"core_schema": core_schema}),
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
        raise RuntimeError(f"dbt build failed with exit code {exit_code}")


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


def _apply_snapshot(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    published: PublishedDelivery,
    *,
    workgroup: str,
    database: str,
    core_schema: str,
) -> LoadedSnapshot:
    staged = _stage(
        s3,
        redshift,
        published,
        workgroup=workgroup,
        database=database,
        core_schema=core_schema,
    )
    return _load_core(redshift, staged)


@flow(name="historical-snapshot", persist_result=False)
def run_snapshot(source_manifest_uri: str) -> LoadedSnapshot:
    """Stage, load, and build the mart for one published historical snapshot."""
    workgroup = _required_env("RETAIL_REDSHIFT_WORKGROUP")
    database = _required_env("RETAIL_REDSHIFT_DATABASE")
    core_schema = _required_env("RETAIL_CORE_SCHEMA")
    project_dir = _dbt_project()

    with _runner_clients() as (s3, redshift):
        published = _read_manifest(s3, source_manifest_uri, historical_only=True)
        ledger = read_applied_batches(
            redshift, workgroup=workgroup, database=database, core_schema=core_schema
        )
        delivery = published.delivery
        if _verified_replay(
            s3, published, ledger.get((delivery.dataset_id, delivery.batch_id))
        ):
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
            )

    _build_mart(loaded.core_schema, project_dir)
    return loaded


@flow(name="incremental-staging", persist_result=False)
def run_incremental_staging(source_manifest_uri: str) -> dict[str, int]:
    """Stage and verify a published incremental v1 delivery."""
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
        return classify_incremental(
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
    with _runner_clients() as (s3, redshift):
        published = _read_manifest(s3, source_manifest_uri)
        if published.delivery.kind != "incremental":
            raise SnapshotStagingError("Expected an incremental delivery manifest")
        ledger = read_applied_batches(
            redshift, workgroup=workgroup, database=database, core_schema=core_schema
        )
        delivery = published.delivery
        if _verified_replay(
            s3, published, ledger.get((delivery.dataset_id, delivery.batch_id))
        ):
            return AppliedDelivery("already_applied", {})
        return _apply_incremental(
            s3,
            redshift,
            published,
            workgroup=workgroup,
            database=database,
            core_schema=core_schema,
        )


@flow(name="published-deliveries", persist_result=False)
def run_deliveries(
    source_manifest_uri: str | None = None,
    *,
    dataset_id: str | None = None,
    through: date | None = None,
) -> list[DeliveryRun]:
    """Process one published delivery or all normal deliveries through a cutoff."""
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
    with _runner_clients() as (s3, redshift):
        if source_manifest_uri is not None:
            deliveries = [_read_manifest(s3, source_manifest_uri)]
        else:
            assert dataset_id is not None and through is not None
            deliveries = _discover(s3, dataset_id, through)
        if not deliveries:
            return []

        ledger = read_applied_batches(
            redshift, workgroup=workgroup, database=database, core_schema=core_schema
        )
        for published in deliveries:
            delivery = published.delivery
            key = (delivery.dataset_id, delivery.batch_id)
            if _verified_replay(s3, published, ledger.get(key)):
                result = AppliedDelivery("already_applied", {})
            elif delivery.kind == "historical":
                loaded = _apply_snapshot(
                    s3,
                    redshift,
                    published,
                    workgroup=workgroup,
                    database=database,
                    core_schema=core_schema,
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
                )
            get_run_logger().info(
                "Delivery %s/%s %s: %s", *key, result.status, result.counts
            )
            runs.append(DeliveryRun(*key, result.status, result.counts))

    _build_mart(core_schema, project_dir)
    return runs
