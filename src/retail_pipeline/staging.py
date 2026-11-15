"""Stage published deliveries and load historical snapshots from S3."""

import json
import re
from collections import Counter
from dataclasses import dataclass
from hashlib import sha256
from io import BytesIO
from time import monotonic, sleep
from typing import NotRequired, Protocol, TypedDict
from uuid import uuid4

from botocore.exceptions import BotoCoreError, ClientError

from retail_pipeline.application import application_statements, lock_core_sql
from retail_pipeline.classification import (
    CLASSIFICATION_DECISIONS,
    classification_sql,
    classification_summary_sql,
)
from retail_pipeline.discovery import (
    DeliverySelectionError,
    PublishedDelivery,
    SnapshotSource,
    read_published_delivery,
    read_s3_object,
)
from retail_pipeline.manifest import (
    BATCH_ID_PATTERN,
    SNAPSHOT_LAYOUTS,
    parquet_layout,
    validate_parquet_footer,
)
from retail_pipeline.transient import is_aws_throttled, is_transient_aws_error

_REDSHIFT_TYPES = {
    "string": "VARCHAR(65535)",
    "timestamp": "TIMESTAMP",
    "int32": "INTEGER",
    "int64": "BIGINT",
    "money": "DECIMAL(12,2)",
    "bool": "BOOLEAN",
}
_RECOVERY_POLL_SECONDS = 15


class _Statement(TypedDict, total=False):
    Id: str
    SessionId: str
    Status: str
    Error: str
    HasResultSet: bool
    SubStatements: list["_Statement"]


class _StatementResult(TypedDict):
    Records: list[list[dict[str, str | int]]]
    NextToken: NotRequired[str]


class SqlParameter(TypedDict):
    name: str
    value: str


type _ResultRows = list[list[dict[str, str | int]]]


class RedshiftClient(Protocol):
    def batch_execute_statement(
        self,
        *,
        Sqls: list[str],
        SessionKeepAliveSeconds: int,
        WorkgroupName: str = ...,
        Database: str = ...,
        SessionId: str = ...,
        ClientToken: str = ...,
        Parameters: list[SqlParameter] = ...,
    ) -> _Statement: ...

    def describe_statement(self, *, Id: str) -> _Statement: ...

    def get_statement_result(
        self, *, Id: str, NextToken: str = ...
    ) -> _StatementResult: ...


class SnapshotStagingError(ValueError):
    """A published delivery could not be staged or verified."""


class RetryableWarehouseError(SnapshotStagingError):
    """A statement rolled back after a transient database failure."""


class WarehouseWaitTimeout(SnapshotStagingError):
    """Polling stopped before a statement reached a terminal state."""


class WarehouseResultUnavailable(SnapshotStagingError):
    """A completed statement returned no result receipt."""


class WarehouseOutcomeUnknown(SnapshotStagingError):
    """The write could not be reconciled and must not be retried automatically."""


@dataclass(frozen=True)
class StagedSnapshot:
    source_manifest_uri: str
    row_counts: dict[str, int]


@dataclass(frozen=True)
class StagedSnapshotLoad:
    snapshot: StagedSnapshot
    core_schema: str
    session_id: str
    dataset_id: str
    batch_id: str
    manifest_sha256: str
    workgroup: str
    database: str


@dataclass(frozen=True)
class LoadedSnapshot:
    source_manifest_uri: str
    core_schema: str
    row_counts: dict[str, int]
    status: str = "applied"


@dataclass(frozen=True)
class AppliedBatch:
    kind: str
    manifest_sha256: str


@dataclass(frozen=True)
class AppliedDelivery:
    status: str
    counts: dict[str, dict[str, int]]


def read_applied_batches(
    redshift: RedshiftClient,
    *,
    workgroup: str,
    database: str,
    core_schema: str,
    timeout_seconds: int = 300,
) -> dict[tuple[str, str], AppliedBatch]:
    """Read the target ledger, with an absent ledger representing a fresh target."""
    _validate_core_schema(core_schema)
    rows, _ = _run_staging_batch(
        redshift,
        [
            "SELECT COUNT(*) FROM pg_catalog.pg_tables "
            f"WHERE schemaname = '{core_schema}' AND tablename = 'applied_batches'"
        ],
        workgroup=workgroup,
        database=database,
        keep_alive=0,
        timeout_seconds=timeout_seconds,
    )
    if rows[0][0]["longValue"] == 0:
        return {}
    rows, _ = _run_staging_batch(
        redshift,
        [
            "SELECT dataset_id, batch_id, kind, manifest_sha256 "
            f"FROM {core_schema}.applied_batches"
        ],
        workgroup=workgroup,
        database=database,
        keep_alive=0,
        timeout_seconds=timeout_seconds,
    )
    ledger = {
        (str(dataset["stringValue"]), str(batch["stringValue"])): AppliedBatch(
            str(kind["stringValue"]), str(checksum["stringValue"])
        )
        for dataset, batch, kind, checksum in rows
    }
    if len(ledger) != len(rows):
        raise SnapshotStagingError(
            f"Duplicate batch keys in {core_schema}.applied_batches"
        )
    return ledger


def read_published_snapshot(
    s3: SnapshotSource, source_manifest_uri: str
) -> PublishedDelivery:
    """Read the historical manifest and check that its IDs match the S3 key."""
    try:
        return read_published_delivery(s3, source_manifest_uri, historical_only=True)
    except DeliverySelectionError as exc:
        raise SnapshotStagingError(str(exc)) from exc


def stage_snapshot(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    source_manifest_uri: str,
    *,
    workgroup: str,
    database: str,
) -> StagedSnapshot:
    """COPY to session-local tables and report counts after verification."""
    staged, _ = _stage_snapshot(
        s3,
        redshift,
        read_published_snapshot(s3, source_manifest_uri),
        workgroup=workgroup,
        database=database,
        core_schema="core",
        keep_alive=0,
    )
    return staged


def stage_incremental(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    published: PublishedDelivery,
    *,
    workgroup: str,
    database: str,
) -> dict[str, int]:
    """Verify one incremental delivery in session-local staging tables."""
    counts, _ = _stage_incremental(
        s3, redshift, published, workgroup=workgroup, database=database, keep_alive=0
    )
    return counts


def classify_incremental(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    published: PublishedDelivery,
    *,
    workgroup: str,
    database: str,
    core_schema: str,
) -> dict[str, dict[str, int]]:
    """Stage a delivery, then preview its decisions against an existing target."""
    _validate_core_schema(core_schema)
    counts, session_id = _stage_incremental_for_core(
        s3, redshift, published, workgroup=workgroup, database=database
    )

    statements = [classification_sql(entity, core_schema) for entity in counts]
    statements.append(classification_summary_sql(counts))
    batch = redshift.batch_execute_statement(
        Sqls=statements, SessionId=session_id, SessionKeepAliveSeconds=0
    )
    rows = _last_batch_result(redshift, batch["Id"])
    return _read_classification(rows, counts)


def apply_incremental(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    published: PublishedDelivery,
    *,
    workgroup: str,
    database: str,
    core_schema: str,
    fail_before_commit: bool = False,
) -> AppliedDelivery:
    """Apply one published delivery after validating it in a locked transaction."""
    _validate_core_schema(core_schema)
    _validate_manifest_identity(
        published.delivery.dataset_id,
        published.delivery.batch_id,
        published.manifest_sha256,
    )
    counts, session_id = _stage_incremental_for_core(
        s3, redshift, published, workgroup=workgroup, database=database
    )
    rows = _run_core_batch(
        redshift,
        application_statements(
            core_schema, published, counts, fail_before_commit=fail_before_commit
        ),
        session_id,
        batch_key=(published.delivery.dataset_id, published.delivery.batch_id),
        expected=AppliedBatch("incremental", published.manifest_sha256),
        workgroup=workgroup,
        database=database,
        core_schema=core_schema,
    )
    if rows is None:
        return AppliedDelivery("already_applied", {})
    status_rows = [row for row in rows if row[0]["stringValue"] == "_batch"]
    if len(status_rows) != 1:
        raise SnapshotStagingError("Missing incremental application outcome")
    status = status_rows[0][1]["stringValue"]
    if status == "already_applied":
        return AppliedDelivery(status, {})
    errors = {
        "snapshot_required": "Apply the snapshot before incremental deliveries",
        "wrong_dataset": "Incremental delivery and target dataset do not match",
        "changed_batch": "Applied batch key has a different manifest or delivery kind",
    }
    if status in errors:
        raise SnapshotStagingError(errors[status])
    if status not in ("applied", "rejected"):
        raise SnapshotStagingError(
            f"Unexpected incremental application outcome: {status!r}"
        )
    details = [row for row in rows if row[0]["stringValue"] != "_batch"]
    summary = _read_classification(details, counts)
    if status == "rejected":
        raise SnapshotStagingError("Incremental application rejected")
    return AppliedDelivery(status, summary)


def _read_classification(
    rows: list[list[dict[str, str | int]]], staged_counts: dict[str, int]
) -> dict[str, dict[str, int]]:
    summary = {
        entity: dict.fromkeys(CLASSIFICATION_DECISIONS, 0) for entity in staged_counts
    }
    errors = {
        "conflict_in_delivery": "Version conflict within the delivery",
        "conflict_with_core": "Version conflict with current core",
        "invalid_version": "Invalid source version",
        "invalid_core_version": "Invalid core version",
        "duplicate_core_key": "Duplicate business key in core",
    }
    for entity_cell, decision_cell, versions, repeated, sample in rows:
        entity = str(entity_cell["stringValue"])
        decision = str(decision_cell["stringValue"])
        if decision == "classification_count_mismatch":
            raise SnapshotStagingError(
                f"Classification counts differ from staging for {entity}"
            )
        if decision in errors:
            raise SnapshotStagingError(
                f"{errors[decision]} for {entity} key {sample['stringValue']!r}"
            )
        if entity in summary and decision == "quarantined":
            summary[entity][decision] = int(versions["longValue"])
            continue
        if entity not in summary or decision not in summary[entity]:
            raise SnapshotStagingError(
                f"Unexpected classification: {entity} {decision}"
            )
        summary[entity][decision] += int(versions["longValue"])
        summary[entity]["duplicate"] += int(repeated["longValue"])

    classified_counts = {
        entity: sum(counts.values()) for entity, counts in summary.items()
    }
    if classified_counts != staged_counts:
        raise SnapshotStagingError("Classification counts differ from verified staging")
    return summary


def _stage_incremental_for_core(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    published: PublishedDelivery,
    *,
    workgroup: str,
    database: str,
) -> tuple[dict[str, int], str]:
    counts, session_id = _stage_incremental(
        s3, redshift, published, workgroup=workgroup, database=database, keep_alive=60
    )
    if not session_id:
        raise SnapshotStagingError("Redshift did not return the staging session ID")
    return counts, session_id


def _stage_incremental(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    published: PublishedDelivery,
    *,
    workgroup: str,
    database: str,
    keep_alive: int,
) -> tuple[dict[str, int], str | None]:
    if published.delivery.kind != "incremental":
        raise SnapshotStagingError("Expected an incremental delivery manifest")
    statements, expected_counts = _copy_batch(s3, published)
    count_queries = []
    for entity in expected_counts:
        business_key = SNAPSHOT_LAYOUTS[entity][0][0]
        # Redshift text comparisons ignore trailing spaces. Compare byte lengths too.
        count_queries.append(
            f"SELECT '{entity}' AS entity, COUNT(*) AS row_count, "
            "COUNT(CASE WHEN entity_type IS NULL OR record_id IS NULL "
            f"OR entity_type <> '{entity}' "
            f"OR OCTET_LENGTH(entity_type) <> {len(entity)} "
            f"OR record_id <> {business_key} "
            f"OR OCTET_LENGTH(record_id) <> OCTET_LENGTH({business_key}) "
            f"THEN 1 END) AS invalid_ids FROM stage_{entity}"
        )
    statements.append(" UNION ALL ".join(count_queries))
    rows, session_id = _run_staging_batch(
        redshift,
        statements,
        workgroup=workgroup,
        database=database,
        keep_alive=keep_alive,
    )
    actual_counts = _check_row_counts(rows, expected_counts)
    invalid = {
        str(entity["stringValue"]): count["longValue"]
        for entity, _, count in rows
        if count["longValue"] != 0
    }
    if invalid:
        raise SnapshotStagingError(f"Invalid event entity_type or record_id: {invalid}")
    return actual_counts, session_id


def load_snapshot(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    source_manifest_uri: str,
    *,
    workgroup: str,
    database: str,
    core_schema: str,
) -> LoadedSnapshot:
    """Initialize the four tables in the explicitly selected demo schema atomically."""
    _validate_core_schema(core_schema)
    staged = stage_snapshot_for_load(
        s3,
        redshift,
        read_published_snapshot(s3, source_manifest_uri),
        workgroup=workgroup,
        database=database,
        core_schema=core_schema,
    )
    return commit_snapshot_load(redshift, staged)


def stage_snapshot_for_load(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    published: PublishedDelivery,
    *,
    workgroup: str,
    database: str,
    core_schema: str,
) -> StagedSnapshotLoad:
    """Verify staging counts and keep the session open for the core transaction."""
    _validate_core_schema(core_schema)
    if published.delivery.kind != "historical":
        raise SnapshotStagingError("Expected a historical snapshot manifest")
    staged, session_id = _stage_snapshot(
        s3,
        redshift,
        published,
        workgroup=workgroup,
        database=database,
        core_schema=core_schema,
        keep_alive=60,
    )
    if not session_id:
        raise SnapshotStagingError("Redshift did not return the staging session ID")
    return StagedSnapshotLoad(
        staged,
        core_schema,
        session_id,
        published.delivery.dataset_id,
        published.delivery.batch_id,
        published.manifest_sha256,
        workgroup,
        database,
    )


def commit_snapshot_load(
    redshift: RedshiftClient, staged: StagedSnapshotLoad
) -> LoadedSnapshot:
    """Initialize the demo tables once and record the snapshot in one transaction."""
    core_schema = staged.core_schema
    _validate_core_schema(core_schema)
    dataset_id, batch_id, checksum = (
        staged.dataset_id,
        staged.batch_id,
        staged.manifest_sha256,
    )
    _validate_manifest_identity(dataset_id, batch_id, checksum)
    ledger = f"{core_schema}.applied_batches"
    tables = [f"{core_schema}.{entity}" for entity in SNAPSHOT_LAYOUTS]
    # Read the ledger only after taking the locks used to serialize applications.
    batch_key = f"dataset_id = '{dataset_id}' AND batch_id = '{batch_id}'"
    statements = [
        lock_core_sql(core_schema),
        f"""
CREATE TEMP TABLE snapshot_status AS
SELECT CASE
    WHEN EXISTS (SELECT 1 FROM {ledger} WHERE {batch_key}
        AND (kind <> 'historical' OR manifest_sha256 <> '{checksum}'))
        THEN 'changed_batch'
    WHEN EXISTS (SELECT 1 FROM {ledger} WHERE {batch_key}) THEN 'already_applied'
    WHEN EXISTS (SELECT 1 FROM {ledger} WHERE kind = 'incremental') THEN 'daily-applied'
    WHEN EXISTS (SELECT 1 FROM {ledger}) THEN 'different-snapshot'
    ELSE 'applied'
END AS status
""".strip(),
    ]
    can_load = "(SELECT status FROM snapshot_status) = 'applied'"
    statements.extend(
        f"DELETE FROM {table} WHERE {can_load}" for table in reversed(tables)
    )
    for entity, columns in SNAPSHOT_LAYOUTS.items():
        names = [name for name, _ in columns]
        values = names.copy()
        if entity == "customers":
            names.extend(("updated_at", "source_sequence"))
            values.extend(("created_at", "0"))
        if entity == "order_items":
            names.append("discount_amount")
            values.append("COALESCE(discount_amount, CAST(0 AS DECIMAL(12,2)))")
        statements.append(
            f"INSERT INTO {core_schema}.{entity} ({', '.join(names)}) "
            f"SELECT {', '.join(values)} FROM stage_{entity} WHERE {can_load}"
        )
    counts = ", ".join(
        str(staged.snapshot.row_counts[entity]) for entity in SNAPSHOT_LAYOUTS
    )
    statements.extend(
        [
            f"INSERT INTO {ledger} (dataset_id, batch_id, kind, manifest_sha256, "
            "customer_rows, product_rows, order_rows, order_item_rows) "
            f"SELECT '{dataset_id}', '{batch_id}', 'historical', '{checksum}', "
            f"{counts} "
            f"WHERE {can_load}",
            "SELECT status FROM snapshot_status",
        ]
    )
    rows = _run_core_batch(
        redshift,
        statements,
        staged.session_id,
        batch_key=(dataset_id, batch_id),
        expected=AppliedBatch("historical", checksum),
        workgroup=staged.workgroup,
        database=staged.database,
        core_schema=core_schema,
    )
    if rows is None:
        outcome: str | int | None = "already_applied"
    elif len(rows) != 1 or len(rows[0]) != 1:
        raise SnapshotStagingError("Missing snapshot application outcome")
    else:
        outcome = rows[0][0].get("stringValue")
    if outcome == "changed_batch":
        raise SnapshotStagingError(
            "Applied batch key has a different manifest or delivery kind"
        )
    if outcome == "daily-applied":
        raise SnapshotStagingError(
            f"Cannot replace {core_schema}: incremental batches have been applied"
        )
    if outcome == "different-snapshot":
        raise SnapshotStagingError(
            f"Cannot replace {core_schema}: a different snapshot is already recorded"
        )
    if outcome not in ("applied", "already_applied"):
        raise SnapshotStagingError(
            f"Unexpected snapshot application outcome: {outcome!r}"
        )
    return LoadedSnapshot(
        staged.snapshot.source_manifest_uri,
        core_schema,
        staged.snapshot.row_counts,
        str(outcome),
    )


def _validate_manifest_identity(dataset_id: str, batch_id: str, checksum: str) -> None:
    if not (
        re.fullmatch(BATCH_ID_PATTERN, dataset_id)
        and re.fullmatch(BATCH_ID_PATTERN, batch_id)
        and re.fullmatch(r"[0-9a-f]{64}", checksum)
    ):
        raise SnapshotStagingError("Invalid staged manifest identity")


def _validate_core_schema(core_schema: str) -> None:
    if not re.fullmatch(r"core_demo(?:_[a-z0-9_]{1,117})?", core_schema):
        raise SnapshotStagingError(
            "Target schema must be core_demo or core_demo_<name>"
        )


def _stage_snapshot(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    published: PublishedDelivery,
    *,
    workgroup: str,
    database: str,
    core_schema: str,
    keep_alive: int,
) -> tuple[StagedSnapshot, str | None]:
    copy_statements, expected_counts = _copy_batch(s3, published)
    statements = [f"CREATE SCHEMA IF NOT EXISTS {core_schema}"]
    if keep_alive:
        # Create the ledger before the application transaction takes its locks.
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {core_schema}.applied_batches ("
            "dataset_id VARCHAR(1024) NOT NULL, batch_id VARCHAR(1024) NOT NULL, "
            "kind VARCHAR(11) NOT NULL, manifest_sha256 CHAR(64) NOT NULL, "
            "applied_at TIMESTAMP NOT NULL DEFAULT SYSDATE, "
            "customer_rows BIGINT NOT NULL, product_rows BIGINT NOT NULL, "
            "order_rows BIGINT NOT NULL, order_item_rows BIGINT NOT NULL, "
            "PRIMARY KEY (dataset_id, batch_id))"
        )
    count_queries = []
    for entity, columns in SNAPSHOT_LAYOUTS.items():
        core_columns = _sql_columns(columns)
        if entity == "customers":
            core_columns += ", updated_at TIMESTAMP, source_sequence BIGINT"
        if entity == "order_items":
            core_columns += ", discount_amount DECIMAL(12,2)"
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {core_schema}.{entity} ({core_columns})"
        )
        count_queries.append(
            f"SELECT '{entity}' AS entity, COUNT(*) AS row_count FROM stage_{entity}"
        )
    statements.extend(copy_statements)
    statements.append(" UNION ALL ".join(count_queries))
    rows, session_id = _run_staging_batch(
        redshift,
        statements,
        workgroup=workgroup,
        database=database,
        keep_alive=keep_alive,
    )
    actual_counts = _check_row_counts(rows, expected_counts)
    return StagedSnapshot(published.source_manifest_uri, actual_counts), session_id


def _sql_columns(columns: tuple[tuple[str, str], ...]) -> str:
    return ", ".join(f"{name} {_REDSHIFT_TYPES[kind]}" for name, kind in columns)


def _copy_batch(
    s3: SnapshotSource,
    published: PublishedDelivery,
) -> tuple[list[str], Counter[str]]:
    bucket, prefix = published.bucket, published.prefix
    expected_counts: Counter[str] = Counter()
    expected_urls: dict[str, set[str]] = {}
    files_by_entity = {}
    for entry in published.delivery.files:
        expected_counts[entry.entity_type] += entry.row_count
        key = f"{prefix}/{entry.relative_path}"
        expected_urls.setdefault(entry.entity_type, set()).add(f"s3://{bucket}/{key}")
        files_by_entity[entry.entity_type] = entry
        if entry.entity_type == "order_items":
            # Check the exact bytes Redshift will load before submitting any COPY.
            data = read_s3_object(s3, bucket, key)
            if sha256(data).hexdigest() != entry.sha256:
                raise SnapshotStagingError(f"SHA-256 mismatch for s3://{bucket}/{key}")
            validate_parquet_footer(BytesIO(data), entry, kind=published.delivery.kind)

    statements = []
    for entity in SNAPSHOT_LAYOUTS:
        if entity not in files_by_entity:
            continue
        entry = files_by_entity[entity]
        columns = parquet_layout(entry, kind=published.delivery.kind)
        missing_discount = entity == "order_items" and entry.schema_version == 1
        table = f"stage_{entity}_v1" if missing_discount else f"stage_{entity}"
        copy_key = f"{prefix}/copy/{entity}.json"
        _verify_copy_manifest(s3, bucket, copy_key, expected_urls[entity])
        statements.extend(
            [
                f"CREATE TEMP TABLE {table} ({_sql_columns(columns)})",
                f"COPY {table} FROM 's3://{bucket}/{copy_key}' "
                "IAM_ROLE default FORMAT AS PARQUET MANIFEST",
            ]
        )
        if missing_discount:
            statements.append(
                "CREATE TEMP TABLE stage_order_items AS SELECT *, "
                "CAST(0 AS DECIMAL(12,2)) AS discount_amount FROM stage_order_items_v1"
            )
        elif entity == "order_items" and published.delivery.kind == "historical":
            statements.append(
                "SELECT CAST(CASE WHEN COUNT(*) > 0 THEN 'invalid snapshot discount' "
                "ELSE '0' END AS INTEGER) FROM stage_order_items "
                "WHERE discount_amount < 0 OR discount_amount > quantity * unit_price"
            )
    return statements, expected_counts


def _run_staging_batch(
    redshift: RedshiftClient,
    statements: list[str],
    *,
    workgroup: str,
    database: str,
    keep_alive: int,
    timeout_seconds: int = 300,
) -> tuple[_ResultRows, str | None]:
    batch = redshift.batch_execute_statement(
        WorkgroupName=workgroup,
        Database=database,
        Sqls=statements,
        SessionKeepAliveSeconds=keep_alive,
    )
    rows = _last_batch_result(redshift, batch["Id"], timeout_seconds=timeout_seconds)
    return rows, batch.get("SessionId")


def _run_core_batch(
    redshift: RedshiftClient,
    statements: list[str],
    session_id: str,
    *,
    batch_key: tuple[str, str],
    expected: AppliedBatch,
    workgroup: str,
    database: str,
    core_schema: str,
) -> _ResultRows | None:
    """Resolve an uncertain write before allowing a new warehouse attempt."""
    token = uuid4().hex
    statement_id = None
    try:
        statement_id = _submit_core_batch(redshift, statements, session_id, token)
        try:
            return _last_batch_result(redshift, statement_id)
        except (ClientError, BotoCoreError, WarehouseWaitTimeout) as exc:
            if not (
                is_transient_aws_error(exc) or isinstance(exc, WarehouseWaitTimeout)
            ):
                raise
            # Recheck the existing statement before falling back to the ledger.
            return _last_batch_result(
                redshift, statement_id, timeout_seconds=_RECOVERY_POLL_SECONDS
            )
    except (
        ClientError,
        BotoCoreError,
        RetryableWarehouseError,
        WarehouseWaitTimeout,
        WarehouseResultUnavailable,
        WarehouseOutcomeUnknown,
    ) as exc:
        if statement_id is None and not isinstance(exc, WarehouseOutcomeUnknown):
            raise
        reference = (
            f"statement {statement_id}" if statement_id else f"submission token {token}"
        )
        dataset_id, batch_id = batch_key
        unknown = (
            f"Cannot confirm warehouse outcome for {dataset_id}/{batch_id} "
            f"({reference})"
        )
        try:
            ledger = read_applied_batches(
                redshift,
                workgroup=workgroup,
                database=database,
                core_schema=core_schema,
                timeout_seconds=_RECOVERY_POLL_SECONDS,
            )
        except (SnapshotStagingError, ClientError, BotoCoreError) as ledger_error:
            raise WarehouseOutcomeUnknown(
                f"{unknown}; ledger check failed"
            ) from ledger_error
        recorded = ledger.get(batch_key)
        if recorded == expected:
            return None
        if recorded is not None:
            raise SnapshotStagingError(
                "Applied batch key has a different manifest or kind"
            ) from exc
        if isinstance(exc, RetryableWarehouseError):
            raise
        raise WarehouseOutcomeUnknown(
            f"{unknown}; no matching committed ledger entry"
        ) from exc


def _submit_core_batch(
    redshift: RedshiftClient, statements: list[str], session_id: str, token: str
) -> str:
    try:
        batch = redshift.batch_execute_statement(
            Sqls=statements,
            SessionId=session_id,
            SessionKeepAliveSeconds=0,
            ClientToken=token,
        )
    except (ClientError, BotoCoreError) as exc:
        if not is_transient_aws_error(exc):
            raise
        # A capacity rejection on the first SDK attempt rules out a prior submission.
        if (
            isinstance(exc, ClientError)
            and is_aws_throttled(exc)
            and exc.response.get("ResponseMetadata", {}).get("RetryAttempts") == 0
        ):
            raise
        try:
            # Recover the receipt with exactly the original Data API request token.
            batch = redshift.batch_execute_statement(
                Sqls=statements,
                SessionId=session_id,
                SessionKeepAliveSeconds=0,
                ClientToken=token,
            )
        except (ClientError, BotoCoreError) as retry_error:
            # Redshift can reject the replay once the staging session has closed.
            raise WarehouseOutcomeUnknown(
                "Could not recover submission receipt"
            ) from retry_error
    statement_id = batch.get("Id")
    if statement_id is None:
        raise WarehouseOutcomeUnknown("Missing statement ID in submission receipt")
    return statement_id


def _last_batch_result(
    redshift: RedshiftClient, statement_id: str, *, timeout_seconds: int = 300
) -> _ResultRows:
    finished = _wait_for_statement(
        redshift, statement_id, timeout_seconds=timeout_seconds
    )
    substatements = finished.get("SubStatements", [])
    if not substatements or not substatements[-1].get("HasResultSet"):
        raise WarehouseResultUnavailable(
            f"Missing final query result for {statement_id}"
        )
    result_id = substatements[-1]["Id"]
    result = redshift.get_statement_result(Id=result_id)
    rows = result["Records"]
    while token := result.get("NextToken"):
        result = redshift.get_statement_result(Id=result_id, NextToken=token)
        rows.extend(result["Records"])
    return rows


def _check_row_counts(
    rows: list[list[dict[str, str | int]]], expected_counts: Counter[str]
) -> dict[str, int]:
    actual_counts = {
        str(row[0]["stringValue"]): int(row[1]["longValue"]) for row in rows
    }
    if actual_counts != expected_counts:
        raise SnapshotStagingError(
            f"Staged row counts differ from the source manifest. "
            f"Expected {dict(expected_counts)}, got {actual_counts}"
        )
    return actual_counts


def _verify_copy_manifest(
    s3: SnapshotSource, bucket: str, key: str, expected_urls: set[str]
) -> None:
    try:
        manifest: object = json.loads(read_s3_object(s3, bucket, key).decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise SnapshotStagingError(f"Invalid COPY manifest: {key}") from exc
    if not isinstance(manifest, dict) or not isinstance(manifest.get("entries"), list):
        raise SnapshotStagingError(f"Missing COPY manifest entries: {key}")
    seen_urls: set[str] = set()
    for entry in manifest["entries"]:
        if not isinstance(entry, dict):
            raise SnapshotStagingError(f"Invalid COPY manifest entry: {key}")
        url = entry.get("url")
        meta = entry.get("meta")
        length = meta.get("content_length") if isinstance(meta, dict) else None
        if (
            not isinstance(url, str)
            or url not in expected_urls
            or url in seen_urls
            or entry.get("mandatory") is not True
            or type(length) is not int
            or length <= 0
        ):
            raise SnapshotStagingError(f"Invalid COPY manifest entry in {key}: {url!r}")
        seen_urls.add(url)
    if seen_urls != expected_urls:
        raise SnapshotStagingError(f"COPY manifest omits source files: {key}")


def _wait_for_statement(
    client: RedshiftClient, statement_id: str, *, timeout_seconds: int = 300
) -> _Statement:
    deadline = monotonic() + timeout_seconds
    while monotonic() < deadline:
        result = client.describe_statement(Id=statement_id)
        status = result["Status"]
        if status == "FINISHED":
            return result
        if status not in {"SUBMITTED", "PICKED", "STARTED"}:
            error = result.get("Error", "")
            message = f"Redshift statement {statement_id}: {status}. {error}"
            if status in {"FAILED", "ABORTED"} and re.search(
                r"serializable isolation violation|deadlock detected|"
                r"could not obtain lock|"
                r"\bSQLSTATE[\s:=]+(?:40001|40P01|55P03)\b",
                error,
                re.IGNORECASE,
            ):
                raise RetryableWarehouseError(message)
            raise SnapshotStagingError(message)
        sleep(2)
    raise WarehouseWaitTimeout(
        f"Redshift statement {statement_id} may still be running after the timeout"
    )
