import json
from collections.abc import Iterator
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from prefect.testing.utilities import prefect_test_harness

from retail_pipeline import flows
from retail_pipeline.cli import main
from retail_pipeline.staging import SnapshotStagingError, load_snapshot, stage_snapshot

BUCKET = "retail-raw"
PREFIX = "raw/demo-v1/snapshot-001"
SOURCE_URI = f"s3://{BUCKET}/{PREFIX}/manifest.json"
COUNTS = {"customers": 800, "products": 50, "orders": 4900, "order_items": 8747}


class PublishedObjects:
    def __init__(self) -> None:
        files = []
        self.objects = {}
        for entity, count in COUNTS.items():
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
            self.objects[f"{PREFIX}/copy/{entity}.json"] = {
                "entries": [
                    {
                        "url": f"s3://{BUCKET}/{PREFIX}/{path}",
                        "mandatory": True,
                        "meta": {"content_length": 10},
                    }
                ]
            }
        self.objects[f"{PREFIX}/manifest.json"] = {
            "dataset_id": "demo-v1",
            "batch_id": "snapshot-001",
            "kind": "historical",
            "arrival_date": "2026-11-10",
            "files": files,
        }

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        assert Bucket == BUCKET
        return {"Body": BytesIO(json.dumps(self.objects[Key]).encode())}


class RedshiftBatch:
    def __init__(self, counts: dict[str, int]) -> None:
        self.counts = counts
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

        self.statements = Sqls
        return {"Id": "batch-id", "SessionId": "verified-staging-session"}

    def describe_statement(self, *, Id: str) -> dict:
        return {
            "Status": "FINISHED",
            "SubStatements": [{"Id": "batch-id:13", "HasResultSet": True}],
        }

    def get_statement_result(self, *, Id: str) -> dict:
        assert Id == "batch-id:13"
        return {
            "Records": [
                [{"stringValue": entity}, {"longValue": count}]
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


def test_core_load_uses_verified_session_and_replaces_only_demo_tables() -> None:
    redshift = RedshiftBatch(COUNTS)

    loaded = load_snapshot(
        PublishedObjects(),
        redshift,
        SOURCE_URI,
        workgroup="retail-dev",
        database="retail",
        core_schema="core_demo",
    )

    assert loaded.row_counts == COUNTS
    assert loaded.core_schema == "core_demo"
    assert redshift.core_statements[1:5] == [
        f"DELETE FROM core_demo.{entity}" for entity in reversed(COUNTS)
    ]
    inserts = redshift.core_statements[5:]
    assert [sql.split()[2] for sql in inserts] == [
        f"core_demo.{entity}" for entity in COUNTS
    ]
    assert "created_at, 0 FROM stage_customers" in inserts[0]
    assert "0.00 FROM stage_order_items" in inserts[-1]


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
        assert redshift.core_statements[1] == "DELETE FROM core_demo_flow.order_items"
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
