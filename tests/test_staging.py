import json
from base64 import b64encode
from collections.abc import Iterator
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from prefect.testing.utilities import prefect_test_harness

from retail_pipeline import flows
from retail_pipeline.cli import main
from retail_pipeline.discovery import DeliverySelectionError, read_published_delivery
from retail_pipeline.staging import (
    AppliedBatch,
    AppliedDelivery,
    LoadedSnapshot,
    SnapshotStagingError,
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

    def batch_execute_statement(
        self,
        *,
        Sqls: list[str],
        SessionKeepAliveSeconds: int,
        WorkgroupName: str = "",
        Database: str = "",
        SessionId: str = "",
    ) -> dict:
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
@pytest.mark.parametrize("failure", [None, "stage", "dbt"])
def test_run_snapshot_cli_runs_dbt_only_after_verified_core_load(
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
    process.stdout = ["dbt build complete\n"]
    process.wait.return_value = 1 if failure == "dbt" else 0

    def start_dbt(command: list[str], **kwargs: object) -> MagicMock:
        assert redshift.core_statements[2].startswith(
            "DELETE FROM core_demo_flow.order_items WHERE "
        )
        assert command[:5] == [
            "dbt",
            "--no-use-colors",
            "build",
            "--project-dir",
            str(project_dir),
        ]
        assert command[5] == "--vars"
        assert json.loads(command[6]) == {"core_schema": "core_demo_flow"}
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
    elif failure == "dbt":
        with pytest.raises(RuntimeError, match="dbt build failed with exit code 1"):
            main()
        fake_subprocess.Popen.assert_called_once()
    else:
        main()
        fake_subprocess.Popen.assert_called_once()


@pytest.mark.usefixtures("prefect_server")
@pytest.mark.parametrize(
    "scenario", ["fresh", "resume", "changed-manifest", "changed-file"]
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
    monkeypatch.setattr(
        "sys.argv",
        [
            "retail-pipeline",
            "run-deliveries",
            "--dataset-id",
            "demo-v1",
            "--through",
            "2026-11-10",
        ],
    )

    s3 = PublishedObjects()
    for batch in ("daily-002", "daily-001", "late-001"):
        s3.objects.update(PublishedObjects(incremental=True, batch_id=batch).objects)
    ledger = {}
    if scenario != "fresh":
        for batch in ("snapshot-001", "daily-001"):
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
    clients = {"s3": s3, "redshift-data": SimpleNamespace(close=lambda: None)}
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
