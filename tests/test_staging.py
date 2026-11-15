import json
from base64 import b64encode
from collections.abc import Iterator
from contextlib import nullcontext
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError, ReadTimeoutError
from prefect import flow
from prefect.testing.utilities import prefect_test_harness

from retail_pipeline import flows, staging
from retail_pipeline.audit import AuditBuffer
from retail_pipeline.cli import main
from retail_pipeline.discovery import DeliverySelectionError, read_published_delivery
from retail_pipeline.manifest import ManifestValidationError
from retail_pipeline.staging import (
    AppliedBatch,
    AppliedDelivery,
    LoadedSnapshot,
    SnapshotStagingError,
    SqlParameter,
    WarehouseOutcomeUnknown,
    load_snapshot,
    stage_incremental,
    stage_snapshot,
)

BUCKET = "retail-raw"
PREFIX = "raw/demo-v1/snapshot-001"
SOURCE_URI = f"s3://{BUCKET}/{PREFIX}/manifest.json"
COUNTS = {"customers": 800, "products": 50, "orders": 4900, "order_items": 8747}


class PublishedObjects:
    def __init__(
        self, *, incremental: bool = False, batch_id: str | None = None
    ) -> None:
        files = []
        self.objects = {}
        batch_id = batch_id or ("daily-001" if incremental else "snapshot-001")
        prefix = f"raw/demo-v1/{batch_id}"
        counts = {"orders": 1} if incremental else COUNTS
        for entity, count in counts.items():
            path = f"{entity}/part-001.parquet"
            files.append(
                {
                    "path": path,
                    "entity_type": entity,
                    "schema_version": 1,
                    "row_count": count,
                    "sha256": "a" * 64,
                }
            )
            self.objects[f"{prefix}/copy/{entity}.json"] = {
                "entries": [
                    {
                        "url": f"s3://{BUCKET}/{prefix}/{path}",
                        "mandatory": True,
                        "meta": {"content_length": 10},
                    }
                ]
            }
        self.objects[f"{prefix}/manifest.json"] = {
            "dataset_id": "demo-v1",
            "batch_id": batch_id,
            "kind": "incremental" if incremental else "historical",
            "arrival_date": "2026-11-10",
            "files": files,
        }

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        assert Bucket == BUCKET
        return {"Body": BytesIO(json.dumps(self.objects[Key]).encode())}

    def get_paginator(self, operation_name: str) -> SimpleNamespace:
        assert operation_name == "list_objects_v2"
        return SimpleNamespace(
            paginate=lambda **_: [
                {"Contents": [{"Key": key} for key in sorted(self.objects)]}
            ]
        )

    def head_object(self, *, Bucket: str, Key: str, ChecksumMode: str) -> dict:
        assert Bucket == BUCKET and ChecksumMode == "ENABLED"
        assert Key.endswith(".parquet")
        return {"ChecksumSHA256": b64encode(bytes.fromhex("a" * 64)).decode("ascii")}


class RedshiftBatch:
    def __init__(
        self,
        counts: dict[str, int],
        *,
        invalid_ids: int | None = None,
        core_outcome: str = "applied",
    ) -> None:
        self.counts = counts
        self.invalid_ids = invalid_ids
        self.core_outcome = core_outcome
        self.statements: list[str] = []
        self.core_statements: list[str] = []
        self.audit_records: list[dict] = []

    def batch_execute_statement(
        self,
        *,
        Sqls: list[str],
        SessionKeepAliveSeconds: int,
        WorkgroupName: str = "",
        Database: str = "",
        SessionId: str = "",
        ClientToken: str = "",
        Parameters: list[SqlParameter] | None = None,
    ) -> dict:
        if Parameters:
            assert not SessionId and SessionKeepAliveSeconds == 0
            assert Sqls[-1].startswith("INSERT INTO ")
            self.audit_records.extend(json.loads(Parameters[0]["value"]))
            return {"Id": "audit-batch"}
        if SessionId:
            assert SessionId == "verified-staging-session"
            assert SessionKeepAliveSeconds == 0
            self.core_statements = Sqls
            return {"Id": "core-batch"}

        if "FROM pg_catalog.pg_tables" in Sqls[0]:
            return {"Id": "ledger-catalog"}

        self.statements = Sqls
        return {"Id": "batch-id", "SessionId": "verified-staging-session"}

    def describe_statement(self, *, Id: str) -> dict:
        return {
            "Status": "FINISHED",
            "SubStatements": [{"Id": f"{Id}:last", "HasResultSet": True}],
        }

    def get_statement_result(self, *, Id: str, NextToken: str = "") -> dict:
        if Id == "core-batch:last":
            return {"Records": [[{"stringValue": self.core_outcome}]]}
        if Id == "ledger-catalog:last":
            return {"Records": [[{"longValue": 0}]]}
        assert Id == "batch-id:last"
        invalid = [] if self.invalid_ids is None else [{"longValue": self.invalid_ids}]
        return {
            "Records": [
                [{"stringValue": entity}, {"longValue": count}, *invalid]
                for entity, count in self.counts.items()
            ]
        }


def test_stages_exact_files_from_s3_and_checks_all_entity_counts() -> None:
    redshift = RedshiftBatch(COUNTS)

    staged = stage_snapshot(
        PublishedObjects(),
        redshift,
        SOURCE_URI,
        workgroup="retail-dev",
        database="retail",
    )

    assert staged.row_counts == COUNTS
    assert [sql for sql in redshift.statements if sql.startswith("COPY ")] == [
        f"COPY stage_{entity} FROM 's3://{BUCKET}/{PREFIX}/copy/{entity}.json' "
        "IAM_ROLE default FORMAT AS PARQUET MANIFEST"
        for entity in COUNTS
    ]
    assert "discount_amount" not in next(
        sql
        for sql in redshift.statements
        if sql.startswith("CREATE TEMP TABLE stage_order_items")
    )
    assert any(
        sql.startswith("CREATE TABLE IF NOT EXISTS core.order_items")
        and "discount_amount DECIMAL(12,2)" in sql
        for sql in redshift.statements
    )


def test_rejects_copy_manifest_with_an_unlisted_file_before_running_sql() -> None:
    s3 = PublishedObjects()
    s3.objects[f"{PREFIX}/copy/orders.json"]["entries"][0]["url"] += ".other"
    redshift = RedshiftBatch(COUNTS)

    with pytest.raises(SnapshotStagingError, match="Invalid COPY manifest entry"):
        stage_snapshot(
            s3, redshift, SOURCE_URI, workgroup="retail-dev", database="retail"
        )

    assert redshift.statements == []


def test_reports_row_count_mismatch() -> None:
    redshift = RedshiftBatch({**COUNTS, "orders": 4899})

    with pytest.raises(SnapshotStagingError, match="Staged row counts differ"):
        stage_snapshot(
            PublishedObjects(),
            redshift,
            SOURCE_URI,
            workgroup="retail-dev",
            database="retail",
        )


@pytest.mark.parametrize(
    ("count", "invalid_ids", "error"),
    [
        (1, 0, None),
        (0, 0, "Staged row counts differ"),
        (1, 1, "Invalid event entity_type or record_id"),
    ],
)
def test_incremental_staging_checks_row_counts_and_event_identity(
    count: int, invalid_ids: int, error: str | None
) -> None:
    s3 = PublishedObjects(incremental=True)
    batch = RedshiftBatch({"orders": count}, invalid_ids=invalid_ids)
    prefix = "raw/demo-v1/daily-001"
    published = read_published_delivery(s3, f"s3://{BUCKET}/{prefix}/manifest.json")

    if error:
        with pytest.raises(SnapshotStagingError, match=error):
            stage_incremental(
                s3, batch, published, workgroup="retail-dev", database="retail"
            )
    else:
        assert stage_incremental(
            s3, batch, published, workgroup="retail-dev", database="retail"
        ) == {"orders": 1}

    create_table, copy, audit = batch.statements
    assert create_table.startswith("CREATE TEMP TABLE stage_orders (event_id ")
    assert "arrival_at TIMESTAMP, order_id VARCHAR" in create_table
    assert (
        copy == f"COPY stage_orders FROM 's3://{BUCKET}/{prefix}/copy/orders.json' "
        "IAM_ROLE default FORMAT AS PARQUET MANIFEST"
    )
    assert "entity_type <> 'orders'" in audit and "record_id <> order_id" in audit
    assert batch.core_statements == []


def test_core_load_uses_verified_session_and_replaces_only_demo_tables() -> None:
    redshift = RedshiftBatch(COUNTS)
    s3 = PublishedObjects()
    checksum = sha256(
        json.dumps(s3.objects[f"{PREFIX}/manifest.json"]).encode()
    ).hexdigest()

    loaded = load_snapshot(
        s3,
        redshift,
        SOURCE_URI,
        workgroup="retail-dev",
        database="retail",
        core_schema="core_demo",
    )

    assert loaded.row_counts == COUNTS
    assert loaded.core_schema == "core_demo"
    lock, status, *mutations, outcome = redshift.core_statements
    assert lock.startswith("LOCK TABLE core_demo.applied_batches, core_demo.customers,")
    deletes = mutations[:4]
    assert [sql.split()[2] for sql in deletes] == [
        f"core_demo.{entity}" for entity in reversed(COUNTS)
    ]
    inserts = mutations[4:8]
    assert [sql.split()[2] for sql in inserts] == [
        f"core_demo.{entity}" for entity in COUNTS
    ]
    assert "created_at, 0 FROM stage_customers" in inserts[0]
    assert "0.00 FROM stage_order_items" in inserts[-1]
    for sql in deletes + inserts:
        assert "WHERE (SELECT status FROM snapshot_status) = 'applied'" in sql
    ledger_insert = mutations[-1]
    assert ledger_insert.startswith("INSERT INTO core_demo.applied_batches")
    assert (
        f"'demo-v1', 'snapshot-001', 'historical', '{checksum}', 800, 50, 4900, 8747"
        in ledger_insert
    )
    assert "WHERE (SELECT status FROM snapshot_status) = 'applied'" in ledger_insert
    assert "THEN 'daily-applied'" in status
    assert "THEN 'already_applied'" in status
    assert outcome == "SELECT status FROM snapshot_status"


@pytest.mark.parametrize(
    ("outcome", "message"),
    [
        ("daily-applied", "incremental batches have been applied"),
        ("different-snapshot", "a different snapshot is already recorded"),
    ],
)
def test_core_load_reports_ledger_protection(outcome: str, message: str) -> None:
    with pytest.raises(SnapshotStagingError, match=message):
        load_snapshot(
            PublishedObjects(),
            RedshiftBatch(COUNTS, core_outcome=outcome),
            SOURCE_URI,
            workgroup="retail-dev",
            database="retail",
            core_schema="core_demo",
        )


def test_core_load_stops_before_mutation_when_staged_counts_differ() -> None:
    redshift = RedshiftBatch({**COUNTS, "orders": 4899})

    with pytest.raises(SnapshotStagingError, match="Staged row counts differ"):
        load_snapshot(
            PublishedObjects(),
            redshift,
            SOURCE_URI,
            workgroup="retail-dev",
            database="retail",
            core_schema="core_demo",
        )
    assert not redshift.core_statements


def test_core_load_rejects_target_outside_demo_namespace() -> None:
    redshift = RedshiftBatch(COUNTS)

    with pytest.raises(SnapshotStagingError, match="Target schema must be core_demo"):
        load_snapshot(
            PublishedObjects(),
            redshift,
            SOURCE_URI,
            workgroup="retail-dev",
            database="retail",
            core_schema="core",
        )
    assert not redshift.statements


@pytest.fixture(scope="module")
def prefect_server() -> Iterator[None]:
    with prefect_test_harness():
        yield


@pytest.mark.usefixtures("prefect_server")
@pytest.mark.parametrize(
    ("failure", "expected_attempts"),
    [("throttled", 2), ("unavailable", 3), ("checksum", 1)],
)
def test_prefect_retries_transient_s3_checks_only(
    failure: str, expected_attempts: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    s3 = PublishedObjects(incremental=True)
    key = "raw/demo-v1/daily-001/manifest.json"
    published = read_published_delivery(s3, f"s3://{BUCKET}/{key}")
    head = MagicMock(wraps=s3.head_object)
    if failure == "throttled":
        head.side_effect = [
            ClientError({"Error": {"Code": "SlowDown"}}, "HeadObject"),
            s3.head_object(
                Bucket=BUCKET,
                Key="raw/demo-v1/daily-001/orders/part-001.parquet",
                ChecksumMode="ENABLED",
            ),
        ]
    elif failure == "unavailable":
        head.side_effect = ClientError(
            {"ResponseMetadata": {"HTTPStatusCode": 503}}, "HeadObject"
        )
    else:
        head.return_value = {}
    monkeypatch.setattr(s3, "head_object", head)
    verify = flows._verified_replay.with_options(retry_delay_seconds=0)

    if failure == "throttled":
        audit = AuditBuffer()

        @flow
        def verify_with_audit() -> bool:
            return verify(s3, published, None, audit=audit)

        assert verify_with_audit() is False
        assert [(row["attempt"], row["status"]) for row in audit.records] == [
            (1, "failed"),
            (2, "succeeded"),
        ]
        assert audit.records[0]["failure_reason"] == "ClientError: SlowDown"
        assert audit.records[1]["row_counts"] == {"orders": 1}
        assert audit.records[0]["task_run_id"] == audit.records[1]["task_run_id"]
    elif failure == "unavailable":
        with pytest.raises(ClientError):
            verify(s3, published, None)
    else:
        with pytest.raises(DeliverySelectionError, match="SHA-256 mismatch"):
            verify(s3, published, None)
    assert head.call_count == expected_attempts


@pytest.mark.usefixtures("prefect_server")
def test_prefect_rejects_unsupported_schema_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    s3 = PublishedObjects(incremental=True)
    key = "raw/demo-v1/daily-001/manifest.json"
    s3.objects[key]["files"][0]["schema_version"] = 2
    read = MagicMock(wraps=s3.get_object)
    monkeypatch.setattr(s3, "get_object", read)
    read_manifest = flows._read_manifest.with_options(retry_delay_seconds=0)

    with pytest.raises(ManifestValidationError, match="unsupported incremental schema"):
        read_manifest(s3, f"s3://{BUCKET}/{key}")
    read.assert_called_once_with(Bucket=BUCKET, Key=key)


@pytest.mark.usefixtures("prefect_server")
@pytest.mark.parametrize(
    "outcome", ["lost-receipt", "ledger-confirms", "unknown", "rolled-back"]
)
def test_prefect_resolves_warehouse_outcome_before_resubmitting(
    outcome: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    s3 = PublishedObjects()
    published = read_published_delivery(s3, SOURCE_URI)
    redshift = RedshiftBatch(COUNTS)
    original_submit = redshift.batch_execute_statement
    original_describe = redshift.describe_statement
    original_result = redshift.get_statement_result
    tokens = []
    staging_batches = []
    core_polls = []
    ledger = {}

    def submit(**args):
        if not args.get("SessionId"):
            staging_batches.append(args)
            return original_submit(**args)
        tokens.append(args["ClientToken"])
        if outcome == "lost-receipt" and len(tokens) == 1:
            raise ReadTimeoutError(endpoint_url="https://redshift-data.example")
        return original_submit(**args)

    def describe(*, Id):
        if Id == "core-batch":
            core_polls.append(Id)
            if outcome == "rolled-back" and len(tokens) == 1:
                return {"Status": "FAILED", "Error": "Serializable isolation violation"}
        return original_describe(Id=Id)

    def result(*, Id):
        if Id == "core-batch:last" and outcome in {"ledger-confirms", "unknown"}:
            raise ReadTimeoutError(endpoint_url="https://redshift-data.example")
        return original_result(Id=Id)

    if outcome == "ledger-confirms":
        ledger[("demo-v1", "snapshot-001")] = AppliedBatch(
            "historical", published.manifest_sha256
        )
    read_ledger = MagicMock(return_value=ledger)
    monkeypatch.setattr(staging, "read_applied_batches", read_ledger)
    monkeypatch.setattr(redshift, "batch_execute_statement", submit)
    monkeypatch.setattr(redshift, "describe_statement", describe)
    monkeypatch.setattr(redshift, "get_statement_result", result)

    apply = flows._apply_snapshot.with_options(retry_delay_seconds=0)
    settings = {
        "workgroup": "retail-dev",
        "database": "retail",
        "core_schema": "core_demo",
    }
    if outcome == "unknown":
        with pytest.raises(
            WarehouseOutcomeUnknown, match="no matching committed ledger"
        ):
            apply(s3, redshift, published, **settings)
    else:
        loaded = apply(s3, redshift, published, **settings)
        assert loaded.status == ("already_applied" if ledger else "applied")

    if outcome == "lost-receipt":
        assert len(tokens) == 2 and tokens[0] == tokens[1]
    elif outcome == "rolled-back":
        assert len(tokens) == 2 and tokens[0] != tokens[1]
    else:
        assert len(tokens) == 1
    assert len(staging_batches) == (2 if outcome == "rolled-back" else 1)
    assert len(core_polls) == (1 if outcome == "lost-receipt" else 2)
    assert read_ledger.call_count == (0 if outcome == "lost-receipt" else 1)


@pytest.mark.usefixtures("prefect_server")
def test_prefect_rejects_incremental_version_conflict_without_retry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    s3 = PublishedObjects(incremental=True)
    redshift = RedshiftBatch({"orders": 1}, invalid_ids=0)
    uri = f"s3://{BUCKET}/raw/demo-v1/daily-001/manifest.json"
    published = read_published_delivery(s3, uri)
    original_result = redshift.get_statement_result

    def result(*, Id):
        if Id == "core-batch:last":
            return {
                "Records": [
                    [{"stringValue": "_batch"}, {"stringValue": "rejected"}],
                    [
                        {"stringValue": "orders"},
                        {"stringValue": "conflict_with_core"},
                        {"longValue": 1},
                        {"longValue": 0},
                        {"stringValue": "order-1"},
                    ],
                ]
            }
        return original_result(Id=Id)

    submit = MagicMock(wraps=redshift.batch_execute_statement)
    monkeypatch.setattr(redshift, "batch_execute_statement", submit)
    monkeypatch.setattr(redshift, "get_statement_result", result)
    apply = flows._apply_incremental.with_options(retry_delay_seconds=0)

    with pytest.raises(
        SnapshotStagingError, match="Version conflict with current core"
    ):
        apply(
            s3,
            redshift,
            published,
            workgroup="retail-dev",
            database="retail",
            core_schema="core_demo",
        )
    assert sum("SessionId" in call.kwargs for call in submit.call_args_list) == 1


def test_before_commit_injects_sql_after_core_and_ledger_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    s3 = PublishedObjects(incremental=True)
    uri = f"s3://{BUCKET}/raw/demo-v1/daily-001/manifest.json"
    published = read_published_delivery(s3, uri)
    redshift = RedshiftBatch({"orders": 1}, invalid_ids=0)
    original_describe = redshift.describe_statement

    def describe(*, Id: str) -> dict:
        if Id == "core-batch":
            return {"Status": "FAILED", "Error": "injected-before-commit"}
        return original_describe(Id=Id)

    monkeypatch.setattr(redshift, "describe_statement", describe)
    with pytest.raises(SnapshotStagingError, match="injected-before-commit"):
        staging.apply_incremental(
            s3,
            redshift,
            published,
            workgroup="retail-dev",
            database="retail",
            core_schema="core_demo",
            fail_before_commit=True,
        )

    order_insert, ledger_insert, injected_error, outcome = redshift.core_statements[-4:]
    assert order_insert.startswith("INSERT INTO core_demo.orders")
    assert ledger_insert.startswith("INSERT INTO core_demo.applied_batches")
    assert "injected-before-commit" in injected_error
    assert outcome.startswith("SELECT '_batch'")


@pytest.mark.usefixtures("prefect_server")
@pytest.mark.parametrize(
    ("point", "message", "expected_events"),
    [
        ("before-core", "Injected failure before core writes", []),
        ("before-commit", "injected-before-commit", ["rollback"]),
        ("after-commit", "Injected failure after core commit", ["commit"]),
    ],
)
def test_delivery_cli_injects_failure_without_running_dbt(
    point: str,
    message: str,
    expected_events: list[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    s3 = PublishedObjects(incremental=True)
    uri = f"s3://{BUCKET}/raw/demo-v1/daily-001/manifest.json"
    redshift = RedshiftBatch({"orders": 1})
    monkeypatch.setattr(flows, "_runner_clients", lambda: nullcontext((s3, redshift)))
    monkeypatch.setattr(flows, "_dbt_project", lambda: Path("dbt"))
    monkeypatch.setenv("RETAIL_REDSHIFT_WORKGROUP", "retail-dev")
    monkeypatch.setenv("RETAIL_REDSHIFT_DATABASE", "retail")
    monkeypatch.setenv("RETAIL_CORE_SCHEMA", "core_demo")
    monkeypatch.setattr(
        "sys.argv",
        ["retail-pipeline", "run-deliveries", uri, "--inject-failure", point],
    )
    events = []

    def apply(*args, fail_before_commit: bool, **kwargs) -> AppliedDelivery:
        if fail_before_commit:
            events.append("rollback")
            raise SnapshotStagingError("injected-before-commit")
        events.append("commit")
        return AppliedDelivery("applied", {"orders": {"insert": 1}})

    monkeypatch.setattr(flows, "apply_incremental", apply)
    monkeypatch.setattr(flows, "_build_mart", lambda *args: events.append("dbt"))

    expected_error = SnapshotStagingError if point == "before-commit" else RuntimeError
    with pytest.raises(expected_error, match=message):
        main()
    assert events == expected_events
    records = redshift.audit_records
    assert len({record["run_id"] for record in records}) == 1
    assert all(record["batch_id"] == "daily-001" for record in records)
    assert all(
        record["attempt"] == 1 and record["duration_ms"] >= 0 for record in records
    )
    failure_stages = {
        "before-core": "before_core",
        "before-commit": "core_application",
        "after-commit": "after_commit",
    }
    assert records[-1]["stage"] == failure_stages[point]
    assert (
        records[-1]["status"] == "failed" and message in records[-1]["failure_reason"]
    )
    committed = [record for record in records if record["status"] == "committed"]
    assert bool(committed) == (point == "after-commit")
    if committed:
        assert committed[0]["row_counts"] == {"orders": {"insert": 1}}


@pytest.mark.usefixtures("prefect_server")
@pytest.mark.parametrize("failure", [None, "stage", "build", "publish"])
def test_run_snapshot_cli_publishes_only_after_core_load_and_dbt_build(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str | None
) -> None:
    # The runner checkout only needs the dbt project. The publisher owns local data.
    project_dir = tmp_path / "dbt"
    project_dir.mkdir()
    (project_dir / "dbt_project.yml").touch()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RETAIL_DATASET_PATH", str(tmp_path / "missing-dataset"))
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
    monkeypatch.setenv("RETAIL_REDSHIFT_WORKGROUP", "retail-dev")
    monkeypatch.setenv("RETAIL_REDSHIFT_DATABASE", "retail")
    monkeypatch.setenv("RETAIL_CORE_SCHEMA", "core_demo_flow")
    monkeypatch.setattr("sys.argv", ["retail-pipeline", "run-snapshot", SOURCE_URI])

    s3 = PublishedObjects()
    counts = {**COUNTS, "orders": 4899} if failure == "stage" else COUNTS
    redshift = RedshiftBatch(counts)
    monkeypatch.setattr(s3, "close", lambda: None, raising=False)
    monkeypatch.setattr(redshift, "close", lambda: None, raising=False)
    clients = {"s3": s3, "redshift-data": redshift}
    monkeypatch.setattr(
        flows.boto3, "Session", lambda **_: SimpleNamespace(client=clients.__getitem__)
    )

    process = MagicMock()
    process.__enter__.return_value = process
    process.stdout = ["dbt command output\n"]
    commands: list[list[str]] = []
    candidate_vars: dict[str, str] = {}

    def start_dbt(command: list[str], **kwargs: object) -> MagicMock:
        assert redshift.core_statements[2].startswith(
            "DELETE FROM core_demo_flow.order_items WHERE "
        )
        assert command[:2] == ["dbt", "--no-use-colors"]
        assert command[-4:-1] == ["--project-dir", str(project_dir), "--vars"]
        variables = json.loads(command[-1])
        assert variables["core_schema"] == "core_demo_flow"
        assert variables["sales_candidate_suffix"].startswith("_")
        arguments = command[2:-4]
        commands.append(arguments)
        if arguments == ["build"]:
            candidate_vars.update(variables)
            process.wait.return_value = int(failure == "build")
        else:
            assert arguments == ["run-operation", "publish_daily_sales"]
            assert variables == candidate_vars
            process.wait.return_value = int(failure == "publish")
        return process

    # Leave subprocess handling for Prefect itself untouched.
    fake_subprocess = SimpleNamespace(
        Popen=MagicMock(side_effect=start_dbt), PIPE=-1, STDOUT=-2
    )
    monkeypatch.setattr(flows, "subprocess", fake_subprocess)

    if failure == "stage":
        with pytest.raises(SnapshotStagingError, match="Staged row counts differ"):
            main()
        assert redshift.core_statements == []
        fake_subprocess.Popen.assert_not_called()
        assert redshift.audit_records[-1]["stage"] == "core_application"
        assert redshift.audit_records[-1]["status"] == "failed"
    elif failure == "build":
        with pytest.raises(RuntimeError, match="dbt build failed with exit code 1"):
            main()
        assert commands == [["build"]]
        assert redshift.audit_records[-2]["status"] == "committed"
        assert redshift.audit_records[-1]["stage"] == "dbt_build"
        assert redshift.audit_records[-1]["status"] == "failed"
    else:
        if failure == "publish":
            with pytest.raises(RuntimeError, match="publish_daily_sales failed"):
                main()
        else:
            main()
        assert commands == [["build"], ["run-operation", "publish_daily_sales"]]
        assert redshift.audit_records[-3]["status"] == "committed"
        assert redshift.audit_records[-1]["stage"] == "reporting_publication"
        assert redshift.audit_records[-1]["status"] == (
            "failed" if failure == "publish" else "published"
        )


@pytest.mark.usefixtures("prefect_server")
@pytest.mark.parametrize(
    "scenario", ["fresh", "resume", "exact-replay", "changed-manifest", "changed-file"]
)
def test_run_deliveries_gates_warehouse_writes_and_dbt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    scenario: str,
) -> None:
    project_dir = tmp_path / "dbt"
    project_dir.mkdir()
    (project_dir / "dbt_project.yml").touch()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.setenv("RETAIL_RAW_BUCKET", BUCKET)
    monkeypatch.setenv("RETAIL_REDSHIFT_WORKGROUP", "retail-dev")
    monkeypatch.setenv("RETAIL_REDSHIFT_DATABASE", "retail")
    monkeypatch.setenv("RETAIL_CORE_SCHEMA", "core_demo_flow")
    uri = f"s3://{BUCKET}/raw/demo-v1/daily-002/manifest.json"
    arguments = ["--dataset-id", "demo-v1", "--through", "2026-11-10"]
    if scenario == "exact-replay":
        arguments = [uri]
    monkeypatch.setattr("sys.argv", ["retail-pipeline", "run-deliveries", *arguments])

    s3 = PublishedObjects()
    for batch in ("daily-002", "daily-001", "late-001"):
        s3.objects.update(PublishedObjects(incremental=True, batch_id=batch).objects)
    ledger = {}
    if scenario != "fresh":
        applied = ["snapshot-001", "daily-001"]
        if scenario == "exact-replay":
            applied.append("daily-002")
        for batch in applied:
            published = read_published_delivery(
                s3, f"s3://{BUCKET}/raw/demo-v1/{batch}/manifest.json"
            )
            ledger[("demo-v1", batch)] = AppliedBatch(
                published.delivery.kind, published.manifest_sha256
            )
    daily_key = "raw/demo-v1/daily-001/manifest.json"
    if scenario == "changed-manifest":
        s3.objects[daily_key]["files"][0]["row_count"] = 2
    if scenario == "changed-file":
        original_head = s3.head_object
        monkeypatch.setattr(
            s3,
            "head_object",
            lambda **kwargs: (
                {} if "/daily-001/" in kwargs["Key"] else original_head(**kwargs)
            ),
        )

    monkeypatch.setattr(s3, "close", lambda: None, raising=False)
    redshift = RedshiftBatch(COUNTS)
    monkeypatch.setattr(redshift, "close", lambda: None, raising=False)
    clients = {"s3": s3, "redshift-data": redshift}
    monkeypatch.setattr(
        flows.boto3, "Session", lambda **_: SimpleNamespace(client=clients.__getitem__)
    )
    monkeypatch.setattr(flows, "read_applied_batches", lambda *args, **kwargs: ledger)

    events = []

    def snapshot(*args: object, **kwargs: object) -> LoadedSnapshot:
        events.append("snapshot-001")
        return LoadedSnapshot(SOURCE_URI, "core_demo_flow", COUNTS)

    def incremental(s3, redshift, published, **kwargs):
        events.append(published.delivery.batch_id)
        return AppliedDelivery("applied", {"orders": {"insert": 1}})

    monkeypatch.setattr(flows, "_apply_snapshot", snapshot)
    monkeypatch.setattr(flows, "_apply_incremental", incremental)
    monkeypatch.setattr(flows, "_build_mart", lambda *args: events.append("dbt"))

    if scenario == "changed-manifest":
        with pytest.raises(SnapshotStagingError, match="has a different manifest"):
            main()
        assert events == []
    elif scenario == "changed-file":
        with pytest.raises(DeliverySelectionError, match="SHA-256 mismatch"):
            main()
        assert events == []
    elif scenario == "exact-replay":
        main()
        assert json.loads(capsys.readouterr().out) == [
            {
                "dataset_id": "demo-v1",
                "batch_id": "daily-002",
                "status": "already_applied",
                "counts": {},
            }
        ]
        assert events == ["dbt"]
    else:
        main()
        result = json.loads(capsys.readouterr().out)
        assert [row["batch_id"] for row in result] == [
            "snapshot-001",
            "daily-001",
            "daily-002",
        ]
        if scenario == "fresh":
            assert events == ["snapshot-001", "daily-001", "daily-002", "dbt"]
            assert [row["status"] for row in result] == ["applied"] * 3
        else:
            assert events == ["daily-002", "dbt"]
            assert [row["status"] for row in result] == [
                "already_applied",
                "already_applied",
                "applied",
            ]
