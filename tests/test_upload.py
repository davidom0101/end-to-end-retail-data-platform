import json
from base64 import b64encode
from datetime import date
from hashlib import sha256
from pathlib import Path
from typing import BinaryIO

import pytest
from botocore.exceptions import ClientError

from retail_pipeline.manifest import SnapshotFile, SnapshotManifest
from retail_pipeline.upload import SnapshotUploadError, upload_snapshot

PREFIX = "raw/test-v1/snapshot-001"


class MemoryS3:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

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
