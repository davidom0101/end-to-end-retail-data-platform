"""Stage and load published historical snapshots from S3."""

import json
import re
from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from time import monotonic, sleep
from typing import BinaryIO, Protocol, TypedDict

from retail_pipeline.manifest import SNAPSHOT_LAYOUTS, parse_snapshot_manifest

_REDSHIFT_TYPES = {
    "string": "VARCHAR(65535)",
    "timestamp": "TIMESTAMP",
    "int32": "INTEGER",
    "int64": "BIGINT",
    "money": "DECIMAL(12,2)",
    "bool": "BOOLEAN",
}
_SOURCE_URI = re.compile(
    r"s3://([a-z0-9][a-z0-9.-]{1,61}[a-z0-9])/"
    r"(raw/[a-z0-9][a-z0-9_-]*/[a-z0-9][a-z0-9_-]*)/manifest\.json"
)


class _Statement(TypedDict, total=False):
    Id: str
    SessionId: str
    Status: str
    Error: str
    HasResultSet: bool
    SubStatements: list["_Statement"]


class _StatementResult(TypedDict):
    Records: list[list[dict[str, str | int]]]


class _S3Object(TypedDict):
    Body: BinaryIO


class SnapshotSource(Protocol):
    def get_object(self, *, Bucket: str, Key: str) -> _S3Object: ...


class RedshiftClient(Protocol):
    def batch_execute_statement(
        self,
        *,
        Sqls: list[str],
        SessionKeepAliveSeconds: int,
        WorkgroupName: str = ...,
        Database: str = ...,
        SessionId: str = ...,
    ) -> _Statement: ...

    def describe_statement(self, *, Id: str) -> _Statement: ...

    def get_statement_result(self, *, Id: str) -> _StatementResult: ...


class SnapshotStagingError(ValueError):
    """A published delivery could not be staged or verified."""


@dataclass(frozen=True)
class StagedSnapshot:
    source_manifest_uri: str
    row_counts: dict[str, int]


@dataclass(frozen=True)
class LoadedSnapshot:
    source_manifest_uri: str
    core_schema: str
    row_counts: dict[str, int]


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
        source_manifest_uri,
        workgroup=workgroup,
        database=database,
        core_schema="core",
        keep_alive=0,
    )
    return staged


def load_snapshot(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    source_manifest_uri: str,
    *,
    workgroup: str,
    database: str,
    core_schema: str,
) -> LoadedSnapshot:
    """Replace the four tables in the explicitly selected demo schema atomically."""
    if not re.fullmatch(r"core_demo(?:_[a-z0-9_]{1,117})?", core_schema):
        raise SnapshotStagingError(
            "Target schema must be core_demo or core_demo_<name>"
        )
    staged, session_id = _stage_snapshot(
        s3,
        redshift,
        source_manifest_uri,
        workgroup=workgroup,
        database=database,
        core_schema=core_schema,
        keep_alive=60,
    )
    if not session_id:
        raise SnapshotStagingError("Redshift did not return the staging session ID")

    tables = [f"{core_schema}.{entity}" for entity in SNAPSHOT_LAYOUTS]
    # Acquire all write locks before the transaction takes its data snapshot.
    statements = [f"LOCK TABLE {', '.join(tables)}"]
    statements.extend(f"DELETE FROM {table}" for table in reversed(tables))
    for entity, columns in SNAPSHOT_LAYOUTS.items():
        names = [name for name, _ in columns]
        values = names.copy()
        if entity == "customers":
            names.extend(("updated_at", "source_sequence"))
            values.extend(("created_at", "0"))
        if entity == "order_items":
            names.append("discount_amount")
            values.append("0.00")
        statements.append(
            f"INSERT INTO {core_schema}.{entity} ({', '.join(names)}) "
            f"SELECT {', '.join(values)} FROM stage_{entity}"
        )
    batch = redshift.batch_execute_statement(
        Sqls=statements, SessionId=session_id, SessionKeepAliveSeconds=0
    )
    _wait_for_statement(redshift, batch["Id"])
    return LoadedSnapshot(source_manifest_uri, core_schema, staged.row_counts)


def _stage_snapshot(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    source_manifest_uri: str,
    *,
    workgroup: str,
    database: str,
    core_schema: str,
    keep_alive: int,
) -> tuple[StagedSnapshot, str | None]:
    location = _SOURCE_URI.fullmatch(source_manifest_uri)
    if location is None:
        raise SnapshotStagingError(
            "Expected s3://<bucket>/raw/<dataset-id>/<batch-id>/manifest.json"
        )
    bucket, prefix = location.groups()
    source_bytes = _read_object(s3, bucket, f"{prefix}/manifest.json")
    delivery = parse_snapshot_manifest(source_bytes)
    if prefix != f"raw/{delivery.dataset_id}/{delivery.batch_id}":
        raise SnapshotStagingError("Manifest IDs do not match its S3 key")

    expected_counts: Counter[str] = Counter()
    expected_urls: dict[str, set[str]] = {entity: set() for entity in SNAPSHOT_LAYOUTS}
    for entry in delivery.files:
        expected_counts[entry.entity_type] += entry.row_count
        expected_urls[entry.entity_type].add(
            f"s3://{bucket}/{prefix}/{entry.relative_path}"
        )

    statements = [f"CREATE SCHEMA IF NOT EXISTS {core_schema}"]
    count_queries = []
    for entity, columns in SNAPSHOT_LAYOUTS.items():
        copy_key = f"{prefix}/copy/{entity}.json"
        _verify_copy_manifest(s3, bucket, copy_key, expected_urls[entity])
        staging_columns = ", ".join(
            f"{name} {_REDSHIFT_TYPES[kind]}" for name, kind in columns
        )
        core_columns = staging_columns
        if entity == "customers":
            core_columns += ", updated_at TIMESTAMP, source_sequence BIGINT"
        if entity == "order_items":
            core_columns += ", discount_amount DECIMAL(12,2)"
        # Temporary staging keeps retries and concurrent checks isolated.
        statements.extend(
            [
                f"CREATE TABLE IF NOT EXISTS {core_schema}.{entity} ({core_columns})",
                f"CREATE TEMP TABLE stage_{entity} ({staging_columns})",
                f"COPY stage_{entity} FROM 's3://{bucket}/{copy_key}' "
                "IAM_ROLE default FORMAT AS PARQUET MANIFEST",
            ]
        )
        count_queries.append(
            f"SELECT '{entity}' AS entity, COUNT(*) AS row_count FROM stage_{entity}"
        )
    statements.append(" UNION ALL ".join(count_queries))

    batch = redshift.batch_execute_statement(
        WorkgroupName=workgroup,
        Database=database,
        Sqls=statements,
        SessionKeepAliveSeconds=keep_alive,
    )
    statement_id = batch["Id"]
    finished = _wait_for_statement(redshift, statement_id)
    substatements = finished.get("SubStatements", [])
    if not substatements or not substatements[-1].get("HasResultSet"):
        raise SnapshotStagingError(f"Missing count query result for {statement_id}")
    result_id = substatements[-1]["Id"]
    rows = redshift.get_statement_result(Id=result_id)["Records"]
    actual_counts: dict[str, int] = {}
    for entity_cell, count_cell in rows:
        entity = str(entity_cell["stringValue"])
        actual_counts[entity] = int(count_cell["longValue"])
    if actual_counts != expected_counts:
        raise SnapshotStagingError(
            f"Staged row counts differ from the source manifest. "
            f"Expected {dict(expected_counts)}, got {actual_counts}"
        )
    return StagedSnapshot(source_manifest_uri, actual_counts), batch.get("SessionId")


def _read_object(s3: SnapshotSource, bucket: str, key: str) -> bytes:
    with closing(s3.get_object(Bucket=bucket, Key=key)["Body"]) as body:
        return body.read()


def _verify_copy_manifest(
    s3: SnapshotSource, bucket: str, key: str, expected_urls: set[str]
) -> None:
    try:
        manifest: object = json.loads(_read_object(s3, bucket, key).decode("utf-8"))
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


def _wait_for_statement(client: RedshiftClient, statement_id: str) -> _Statement:
    deadline = monotonic() + 300
    while monotonic() < deadline:
        result = client.describe_statement(Id=statement_id)
        status = result["Status"]
        if status == "FINISHED":
            return result
        if status not in {"SUBMITTED", "PICKED", "STARTED"}:
            error = result.get("Error", "")
            raise SnapshotStagingError(
                f"Redshift statement {statement_id}: {status}. {error}"
            )
        sleep(2)
    raise SnapshotStagingError(
        f"Redshift statement {statement_id} may still be running after the timeout"
    )
