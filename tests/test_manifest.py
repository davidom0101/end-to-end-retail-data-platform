import json
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from retail_pipeline.manifest import ManifestValidationError, validate_snapshot_manifest


@pytest.fixture
def snapshot_manifest(tmp_path: Path) -> Path:
    at = datetime(2026, 11, 10, tzinfo=UTC)
    tables = {
        "customers": pa.table(
            {
                "customer_id": ["c1"],
                "created_at": [at],
                "country": ["GB"],
                "customer_segment": ["retail"],
            }
        ),
        "products": pa.table(
            {
                "product_id": ["p1"],
                "name": ["Notebook"],
                "category": ["Stationery"],
                "current_price": pa.array(
                    [Decimal("10.00")], type=pa.decimal128(12, 2)
                ),
                "is_active": [True],
                "updated_at": [at],
                "source_sequence": [0],
            }
        ),
        "orders": pa.table(
            {
                "order_id": ["o1"],
                "customer_id": ["c1"],
                "order_date": [at],
                "status": ["paid"],
                "updated_at": [at],
                "source_sequence": [0],
            }
        ),
        "order_items": pa.table(
            {
                "item_id": ["i1"],
                "order_id": ["o1"],
                "product_id": ["p1"],
                "quantity": pa.array([2], type=pa.int32()),
                "unit_price": pa.array([Decimal("10.00")], type=pa.decimal128(12, 2)),
                "updated_at": [at],
                "source_sequence": [0],
            }
        ),
    }
    entries = []
    for entity, table in tables.items():
        relative_path = f"{entity}/part-001.parquet"
        file = tmp_path / relative_path
        file.parent.mkdir()
        pq.write_table(table, file, compression="snappy")
        entries.append(
            {
                "path": relative_path,
                "entity_type": entity,
                "schema_version": 1,
                "row_count": 1,
                "sha256": sha256(file.read_bytes()).hexdigest(),
            }
        )
    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps(
            {
                "dataset_id": "test-v1",
                "batch_id": "snapshot-001",
                "kind": "historical",
                "arrival_date": "2026-11-10",
                "files": entries,
            }
        ),
        encoding="utf-8",
    )
    return path


def test_valid_snapshot_returns_verified_files(snapshot_manifest: Path) -> None:
    snapshot = validate_snapshot_manifest(snapshot_manifest)

    assert (snapshot.dataset_id, snapshot.batch_id) == ("test-v1", "snapshot-001")
    assert snapshot.sha256 == sha256(snapshot_manifest.read_bytes()).hexdigest()
    assert {file.entity_type: file.row_count for file in snapshot.files} == {
        "customers": 1,
        "products": 1,
        "orders": 1,
        "order_items": 1,
    }


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("sha256", "0" * 64, "SHA-256 mismatch"),
        ("row_count", 2, "expected 2 rows, found 1"),
        ("schema_version", True, "unsupported snapshot schema version"),
        ("path", "../customers/part-001.parquet", "Unsafe Parquet path"),
        ("path", "C:part-001.parquet", "Unsafe Parquet path"),
    ],
)
def test_rejects_invalid_file_entries(
    snapshot_manifest: Path, field: str, value: object, error: str
) -> None:
    manifest = json.loads(snapshot_manifest.read_bytes())
    manifest["files"][0][field] = value
    snapshot_manifest.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ManifestValidationError, match=error):
        validate_snapshot_manifest(snapshot_manifest)


@pytest.mark.parametrize("change", ["column_order", "timestamp"])
def test_rejects_wrong_layout_even_when_checksum_matches(
    snapshot_manifest: Path, change: str
) -> None:
    manifest = json.loads(snapshot_manifest.read_bytes())
    entry = manifest["files"][0]
    file = snapshot_manifest.parent / entry["path"]
    table = pq.read_table(file)
    if change == "column_order":
        table = table.select(
            ["created_at", "customer_id", "country", "customer_segment"]
        )
        error = "unexpected Parquet columns or order"
    else:
        table = table.set_column(
            1, "created_at", table["created_at"].cast(pa.timestamp("ms"))
        )
        error = "incompatible Parquet column 'created_at'"
    pq.write_table(table, file, compression="snappy")
    entry["sha256"] = sha256(file.read_bytes()).hexdigest()
    snapshot_manifest.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ManifestValidationError, match=error):
        validate_snapshot_manifest(snapshot_manifest)
