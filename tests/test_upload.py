import json
import sys
from base64 import b64encode
from datetime import UTC, date, datetime
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import BinaryIO

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError

from retail_pipeline import cli
from retail_pipeline.manifest import SnapshotFile, SnapshotManifest
from retail_pipeline.upload import SnapshotUploadError, upload_snapshot

PREFIX = "raw/test-v1/snapshot-001"


class MemoryS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def close(self) -> None:
        pass

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes | BinaryIO,
        ContentLength: int,
        ChecksumSHA256: str,
        IfNoneMatch: str,
    ) -> None:
        assert IfNoneMatch == "*"
        if Key in self.objects:
            raise self.error("PutObject", 412)
        content = Body if isinstance(Body, bytes) else Body.read()
        assert len(content) == ContentLength
        assert b64encode(sha256(content).digest()).decode("ascii") == ChecksumSHA256
        self.objects[Key] = content

    def head_object(self, *, Bucket: str, Key: str, ChecksumMode: str) -> dict:
        assert ChecksumMode == "ENABLED"
        if Key not in self.objects:
            raise self.error("HeadObject", 404)
        body = self.objects[Key]
        return {
            "ChecksumSHA256": b64encode(sha256(body).digest()).decode("ascii"),
            "ContentLength": len(body),
        }

    @staticmethod
    def error(operation: str, status: int) -> ClientError:
        return ClientError({"ResponseMetadata": {"HTTPStatusCode": status}}, operation)


@pytest.fixture
def snapshot(tmp_path: Path) -> SnapshotManifest:
    manifest = tmp_path / "manifest.json"
    manifest.write_bytes(b'{"dataset_id":"test-v1","batch_id":"snapshot-001"}\n')
    files = []
    for entity, name in (
        ("orders", "part-002"),
        ("orders", "part-001"),
        ("products", "part-001"),
    ):
        relative = f"{entity}/{name}.parquet"
        path = tmp_path / relative
        path.parent.mkdir(exist_ok=True)
        content = f"{entity} {name}".encode()
        path.write_bytes(content)
        files.append(
            SnapshotFile(path, relative, entity, 1, sha256(content).hexdigest())
        )
    return SnapshotManifest(
        manifest,
        "test-v1",
        "snapshot-001",
        date(2026, 11, 10),
        sha256(manifest.read_bytes()).hexdigest(),
        tuple(files),
    )


def test_upload_publishes_exact_copy_entries_and_reuses_existing_objects(
    snapshot: SnapshotManifest,
) -> None:
    s3 = MemoryS3()
    result = upload_snapshot(s3, "retail-bucket", snapshot)

    assert result.source_manifest_uri == f"s3://retail-bucket/{PREFIX}/manifest.json"
    assert list(s3.objects)[-1] == f"{PREFIX}/manifest.json"
    assert s3.objects[f"{PREFIX}/manifest.json"] == snapshot.path.read_bytes()
    assert set(result.copy_manifest_uris) == {"orders", "products"}
    assert json.loads(s3.objects[f"{PREFIX}/copy/orders.json"]) == {
        "entries": [
            {
                "url": f"s3://retail-bucket/{PREFIX}/orders/{part}.parquet",
                "mandatory": True,
                "meta": {"content_length": len(f"orders {part}")},
            }
            for part in ("part-001", "part-002")
        ]
    }

    original = s3.objects.copy()
    assert upload_snapshot(s3, "retail-bucket", snapshot) == result
    assert s3.objects == original


def test_upload_refuses_to_replace_partial_data(snapshot: SnapshotManifest) -> None:
    s3 = MemoryS3()
    conflict = f"{PREFIX}/orders/part-002.parquet"
    s3.objects[conflict] = b"incorrect object"

    with pytest.raises(SnapshotUploadError, match="S3 object differs"):
        upload_snapshot(s3, "retail-bucket", snapshot)

    assert s3.objects[conflict] == b"incorrect object"
    assert f"{PREFIX}/orders/part-001.parquet" in s3.objects
    assert f"{PREFIX}/manifest.json" not in s3.objects


def test_upload_refuses_changed_published_manifest(snapshot: SnapshotManifest) -> None:
    s3 = MemoryS3()
    manifest_key = f"{PREFIX}/manifest.json"
    s3.objects[manifest_key] = b"different source manifest"

    with pytest.raises(SnapshotUploadError, match="S3 object differs"):
        upload_snapshot(s3, "retail-bucket", snapshot)

    assert s3.objects == {manifest_key: b"different source manifest"}


@pytest.mark.parametrize("wrong_order", [False, True])
def test_upload_delivery_validates_incremental_parquet_before_publishing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, wrong_order: bool
) -> None:
    at = datetime(2026, 11, 11, tzinfo=UTC)
    table = pa.table(
        {
            "event_id": ["e1"],
            "entity_type": ["orders"],
            "record_id": ["o1"],
            "operation": ["insert"],
            "event_time": [at],
            "updated_at": [at],
            "source_sequence": [0],
            "arrival_at": [at],
            "order_id": ["o1"],
            "customer_id": ["c1"],
            "order_date": [at],
            "status": ["paid"],
        }
    )
    if wrong_order:
        table = table.select(
            ["record_id", "entity_type", "event_id", *table.column_names[3:]]
        )
    file = tmp_path / "orders.parquet"
    pq.write_table(table, file, compression="snappy")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "dataset_id": "test-v1",
                "batch_id": "daily-001",
                "kind": "incremental",
                "arrival_date": "2026-11-11",
                "files": [
                    {
                        "path": file.name,
                        "entity_type": "orders",
                        "schema_version": 1,
                        "row_count": 1,
                        "sha256": sha256(file.read_bytes()).hexdigest(),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    s3 = MemoryS3()
    monkeypatch.setattr(
        cli.boto3, "Session", lambda **_: SimpleNamespace(client=lambda _: s3)
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "retail-pipeline",
            "upload-delivery",
            str(manifest),
            "--raw-bucket",
            "retail-bucket",
            "--aws-region",
            "eu-west-1",
        ],
    )

    if wrong_order:
        with pytest.raises(SystemExit) as failure:
            cli.main()
        assert failure.value.code == 1
        assert not s3.objects
    else:
        cli.main()
        prefix = "raw/test-v1/daily-001"
        assert list(s3.objects) == [
            f"{prefix}/orders.parquet",
            f"{prefix}/copy/orders.json",
            f"{prefix}/manifest.json",
        ]
        assert s3.objects[f"{prefix}/manifest.json"] == manifest.read_bytes()
