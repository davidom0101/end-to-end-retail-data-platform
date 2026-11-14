"""Publish validated delivery files and Redshift COPY manifests to S3."""

import json
from base64 import b64encode
from dataclasses import dataclass
from hashlib import sha256
from os import fstat
from typing import BinaryIO, Protocol, TypedDict

from botocore.exceptions import ClientError

from retail_pipeline.manifest import SnapshotManifest


class _S3ObjectMetadata(TypedDict, total=False):
    ChecksumSHA256: str
    ContentLength: int


class S3Client(Protocol):
    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes | BinaryIO,
        ContentLength: int,
        ChecksumSHA256: str,
        IfNoneMatch: str,
    ) -> object: ...

    def head_object(
        self, *, Bucket: str, Key: str, ChecksumMode: str
    ) -> _S3ObjectMetadata: ...


class SnapshotUploadError(ValueError):
    """The delivery cannot be published without changing an existing object."""


@dataclass(frozen=True)
class PublishedSnapshot:
    source_manifest_uri: str
    copy_manifest_uris: dict[str, str]


def upload_delivery(
    s3: S3Client, bucket: str, delivery: SnapshotManifest
) -> PublishedSnapshot:
    """Publish a validated delivery, writing its source manifest last."""
    prefix = f"raw/{delivery.dataset_id}/{delivery.batch_id}"
    manifest_key = f"{prefix}/manifest.json"
    manifest_bytes = delivery.path.read_bytes()
    manifest_digest = sha256(manifest_bytes)
    if manifest_digest.hexdigest() != delivery.sha256:
        raise SnapshotUploadError("The source manifest changed after validation")
    manifest_checksum = b64encode(manifest_digest.digest()).decode("ascii")

    try:
        _verify_object(s3, bucket, manifest_key, manifest_checksum, len(manifest_bytes))
    except ClientError as exc:
        if _status(exc) != 404:
            raise

    entries_by_entity: dict[str, list[dict[str, object]]] = {}
    for file in sorted(delivery.files, key=lambda file: file.relative_path):
        key = f"{prefix}/{file.relative_path}"
        checksum = b64encode(bytes.fromhex(file.sha256)).decode("ascii")
        with file.path.open("rb") as body:
            size = fstat(body.fileno()).st_size
            _put_immutable(s3, bucket, key, body, checksum, size)
        entries_by_entity.setdefault(file.entity_type, []).append(
            {
                "url": f"s3://{bucket}/{key}",
                "mandatory": True,
                "meta": {"content_length": size},
            }
        )

    copy_manifest_uris = {}
    for entity, entries in sorted(entries_by_entity.items()):
        key = f"{prefix}/copy/{entity}.json"
        body_bytes = json.dumps({"entries": entries}, separators=(",", ":")).encode()
        checksum = b64encode(sha256(body_bytes).digest()).decode("ascii")
        _put_immutable(s3, bucket, key, body_bytes, checksum, len(body_bytes))
        copy_manifest_uris[entity] = f"s3://{bucket}/{key}"

    _put_immutable(
        s3, bucket, manifest_key, manifest_bytes, manifest_checksum, len(manifest_bytes)
    )
    return PublishedSnapshot(f"s3://{bucket}/{manifest_key}", copy_manifest_uris)


def upload_snapshot(
    s3: S3Client, bucket: str, snapshot: SnapshotManifest
) -> PublishedSnapshot:
    """Publish a validated historical snapshot."""
    return upload_delivery(s3, bucket, snapshot)


def _put_immutable(
    s3: S3Client,
    bucket: str,
    key: str,
    body: bytes | BinaryIO,
    checksum: str,
    size: int,
) -> None:
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentLength=size,
            ChecksumSHA256=checksum,
            IfNoneMatch="*",
        )
    except ClientError as exc:
        if _status(exc) != 412:
            raise
    _verify_object(s3, bucket, key, checksum, size)


def _verify_object(
    s3: S3Client, bucket: str, key: str, checksum: str, size: int
) -> None:
    metadata = s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
    if (
        metadata.get("ChecksumSHA256") != checksum
        or metadata.get("ContentLength") != size
    ):
        raise SnapshotUploadError(
            f"S3 object differs from the expected bytes: s3://{bucket}/{key}"
        )


def _status(error: ClientError) -> int | None:
    status: int | None = error.response.get("ResponseMetadata", {}).get(
        "HTTPStatusCode"
    )
    return status
