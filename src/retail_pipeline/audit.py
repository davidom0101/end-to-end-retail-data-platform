"""Record delivery stages in Prefect and in a separate Redshift transaction."""

import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from time import monotonic
from typing import TypedDict

from botocore.exceptions import ClientError
from prefect import get_run_logger
from prefect.runtime import flow_run, task_run

from retail_pipeline.staging import (
    RedshiftClient,
    _validate_core_schema,
    _wait_for_statement,
)

type BatchKey = tuple[str | None, str | None]
type RowCounts = dict[str, int] | dict[str, dict[str, int]]

_PAYLOAD_LIMIT_BYTES = 60 * 1024


class AuditRecord(TypedDict):
    run_id: str
    task_run_id: str | None
    dataset_id: str | None
    batch_id: str | None
    attempt: int
    stage: str
    status: str
    started_at: str
    finished_at: str
    duration_ms: int
    row_counts: RowCounts | None
    failure_reason: str | None


class AuditBuffer:
    """Share records between synchronous tasks and their delivery flow."""

    def __init__(self) -> None:
        self.records: list[AuditRecord] = []


@dataclass
class StageOutcome:
    status: str = "succeeded"
    row_counts: RowCounts | None = None
    batch_keys: tuple[BatchKey, ...] = ((None, None),)


@contextmanager
def audit_stage(
    audit: AuditBuffer | None,
    stage: str,
    batch_keys: tuple[BatchKey, ...] = ((None, None),),
) -> Iterator[StageOutcome]:
    """Capture one completed phase or task attempt and retain the original error."""
    outcome = StageOutcome(batch_keys=batch_keys)
    if audit is None:
        yield outcome
        return

    run_id = flow_run.get_id()
    assert run_id is not None
    attempt = task_run.get_run_count() or flow_run.get_run_count()
    started_at = datetime.now(UTC)
    start = monotonic()
    reason = None
    try:
        yield outcome
    except Exception as error:
        outcome.status = "failed"
        message = str(error)
        if isinstance(error, ClientError):
            message = error.response.get("Error", {}).get("Code", "AWS error")
        reason = f"{type(error).__name__}: {message}"[:2048]
        raise
    finally:
        finished_at = datetime.now(UTC).isoformat()
        duration_ms = int((monotonic() - start) * 1000)
        logger = get_run_logger()
        for dataset_id, batch_id in outcome.batch_keys:
            record = AuditRecord(
                run_id=run_id,
                task_run_id=task_run.get_id(),
                dataset_id=dataset_id,
                batch_id=batch_id,
                attempt=attempt,
                stage=stage,
                status=outcome.status,
                started_at=started_at.isoformat(),
                finished_at=finished_at,
                duration_ms=duration_ms,
                row_counts=outcome.row_counts,
                failure_reason=reason,
            )
            audit.records.append(record)
            logger.info("pipeline_audit %s", json.dumps(record, ensure_ascii=False))


@contextmanager
def run_audit(
    redshift: RedshiftClient, *, workgroup: str, database: str, core_schema: str
) -> Iterator[AuditBuffer]:
    """Flush completed stages once the run exits, including on a handled failure."""
    audit = AuditBuffer()
    try:
        yield audit
    except Exception:
        try:
            _write_audit(redshift, workgroup, database, core_schema, audit.records)
        except Exception:
            get_run_logger().exception("Could not persist the run audit to Redshift")
        raise
    else:
        _write_audit(redshift, workgroup, database, core_schema, audit.records)


def _write_audit(
    redshift: RedshiftClient,
    workgroup: str,
    database: str,
    core_schema: str,
    records: list[AuditRecord],
) -> None:
    if not records:
        return
    _validate_core_schema(core_schema)
    table = f"{core_schema}.pipeline_run_audit"
    statements = [
        f"CREATE SCHEMA IF NOT EXISTS {core_schema}",
        f"""CREATE TABLE IF NOT EXISTS {table} (
                run_id VARCHAR(36), task_run_id VARCHAR(36),
                dataset_id VARCHAR(1024), batch_id VARCHAR(1024),
                attempt INTEGER, stage VARCHAR(40), status VARCHAR(32),
                started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ,
                duration_ms BIGINT, row_counts SUPER, failure_reason VARCHAR(8192)
            )""",
        f"""INSERT INTO {table} (
                run_id, task_run_id, dataset_id, batch_id, attempt, stage, status,
                started_at, finished_at, duration_ms, row_counts, failure_reason
            )
            SELECT item.run_id::varchar, item.task_run_id::varchar,
                   item.dataset_id::varchar, item.batch_id::varchar,
                   item.attempt::integer, item.stage::varchar, item.status::varchar,
                   item.started_at::varchar::timestamptz,
                   item.finished_at::varchar::timestamptz, item.duration_ms::bigint,
                   item.row_counts, item.failure_reason::varchar
            FROM (SELECT JSON_PARSE(:events) AS events) AS payload,
                 payload.events AS item
            """,
    ]
    for payload in _audit_payloads(records):
        batch = redshift.batch_execute_statement(
            WorkgroupName=workgroup,
            Database=database,
            SessionKeepAliveSeconds=0,
            Sqls=statements,
            Parameters=[{"name": "events", "value": payload}],
        )
        _wait_for_statement(redshift, batch["Id"])


def _audit_payloads(records: list[AuditRecord]) -> Iterator[str]:
    """Keep each JSON parameter within Redshift's VARCHAR size limit."""
    items: list[str] = []
    size = 2  # JSON array brackets.
    for record in records:
        item = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        item_size = len(item.encode("utf-8")) + 1
        if items and size + item_size > _PAYLOAD_LIMIT_BYTES:
            yield "[" + ",".join(items) + "]"
            items = []
            size = 2
        items.append(item)
        size += item_size
    if items:
        yield "[" + ",".join(items) + "]"
