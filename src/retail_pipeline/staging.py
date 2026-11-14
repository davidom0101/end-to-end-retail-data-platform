"""Stage a published historical snapshot and verify the loaded row counts."""

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
        self, *, WorkgroupName: str, Database: str, Sqls: list[str]
    ) -> _Statement: ...

    def describe_statement(self, *, Id: str) -> _Statement: ...

    def get_statement_result(self, *, Id: str) -> _StatementResult: ...


class SnapshotStagingError(ValueError):
    """A published delivery could not be staged or verified."""


@dataclass(frozen=True)
class StagedSnapshot:
    source_manifest_uri: str
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

    statements = ["CREATE SCHEMA IF NOT EXISTS core"]
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
                f"CREATE TABLE IF NOT EXISTS core.{entity} ({core_columns})",
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
        WorkgroupName=workgroup, Database=database, Sqls=statements
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
    return StagedSnapshot(source_manifest_uri, actual_counts)


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
