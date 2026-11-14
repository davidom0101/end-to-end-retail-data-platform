import json
from io import BytesIO

import pytest

from retail_pipeline.staging import SnapshotStagingError, stage_snapshot

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

    def batch_execute_statement(
        self, *, WorkgroupName: str, Database: str, Sqls: list[str]
    ) -> dict:
        self.statements = Sqls
        return {"Id": "batch-id"}

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
