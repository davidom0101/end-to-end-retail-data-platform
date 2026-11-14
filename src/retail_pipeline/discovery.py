"""Select published deliveries before staging or applying data."""

import re
from collections.abc import Iterable
from contextlib import closing
from dataclasses import dataclass
from datetime import date
from typing import BinaryIO, Literal, Protocol, TypedDict

from retail_pipeline.manifest import (
    BATCH_ID_PATTERN,
    BatchDelivery,
    parse_batch_manifest,
    parse_snapshot_manifest,
)

_BUCKET_PATTERN = r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]"
_SOURCE_URI = re.compile(
    rf"s3://({_BUCKET_PATTERN})/(raw/{BATCH_ID_PATTERN}/{BATCH_ID_PATTERN})/manifest\.json"
)
_NORMAL_BATCH_ID = re.compile(r"(?:snapshot|daily)-[0-9]+")


class _S3Object(TypedDict):
    Body: BinaryIO


class _S3ListedObject(TypedDict):
    Key: str


class _S3Page(TypedDict, total=False):
    Contents: list[_S3ListedObject]


class _S3ObjectPaginator(Protocol):
    def paginate(self, *, Bucket: str, Prefix: str) -> Iterable[_S3Page]: ...


class SnapshotSource(Protocol):
    def get_object(self, *, Bucket: str, Key: str) -> _S3Object: ...


class DeliverySource(SnapshotSource, Protocol):
    def get_paginator(
        self, operation_name: Literal["list_objects_v2"], /
    ) -> _S3ObjectPaginator: ...


class DeliverySelectionError(ValueError):
    """The published S3 deliveries cannot be selected as requested."""


@dataclass(frozen=True)
class PublishedDelivery:
    source_manifest_uri: str
    bucket: str
    prefix: str
    delivery: BatchDelivery


def read_s3_object(s3: SnapshotSource, bucket: str, key: str) -> bytes:
    with closing(s3.get_object(Bucket=bucket, Key=key)["Body"]) as body:
        return body.read()


def read_published_delivery(
    s3: SnapshotSource, source_manifest_uri: str, *, historical_only: bool = False
) -> PublishedDelivery:
    """Read one explicitly selected S3 delivery, including a scenario fixture."""
    location = _SOURCE_URI.fullmatch(source_manifest_uri)
    if location is None:
        raise DeliverySelectionError(
            "Expected s3://<bucket>/raw/<dataset-id>/<batch-id>/manifest.json"
        )
    bucket, prefix = location.groups()
    source_bytes = read_s3_object(s3, bucket, f"{prefix}/manifest.json")
    delivery = (
        parse_snapshot_manifest(source_bytes)
        if historical_only
        else parse_batch_manifest(source_bytes)
    )
    if prefix != f"raw/{delivery.dataset_id}/{delivery.batch_id}":
        raise DeliverySelectionError("Manifest IDs do not match its S3 key")
    return PublishedDelivery(source_manifest_uri, bucket, prefix, delivery)


def discover_deliveries(
    s3: DeliverySource, *, bucket: str, dataset_id: str, cutoff: date
) -> list[PublishedDelivery]:
    """List normal deliveries through the inclusive cutoff in application order."""
    if not re.fullmatch(_BUCKET_PATTERN, bucket):
        raise DeliverySelectionError(f"Invalid S3 bucket name: {bucket!r}")
    if not re.fullmatch(BATCH_ID_PATTERN, dataset_id):
        raise DeliverySelectionError(f"Invalid dataset_id: {dataset_id!r}")

    dataset_prefix = f"raw/{dataset_id}/"
    selected: list[PublishedDelivery] = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=dataset_prefix
    ):
        for entry in page.get("Contents", []):
            key = entry["Key"]
            batch_id, _, filename = key.removeprefix(dataset_prefix).partition("/")
            if filename != "manifest.json" or not _NORMAL_BATCH_ID.fullmatch(batch_id):
                continue
            published = read_published_delivery(s3, f"s3://{bucket}/{key}")
            expected_kind = (
                "historical" if batch_id.startswith("snapshot-") else "incremental"
            )
            if published.delivery.kind != expected_kind:
                raise DeliverySelectionError(
                    f"Expected {expected_kind} manifest at {key}"
                )
            if published.delivery.arrival_date <= cutoff:
                selected.append(published)

    snapshots = [item for item in selected if item.delivery.kind == "historical"]
    if len(snapshots) > 1:
        raise DeliverySelectionError(f"Multiple historical snapshots for {dataset_id}")
    daily = [item for item in selected if item.delivery.kind == "incremental"]
    if daily and not snapshots:
        raise DeliverySelectionError(
            f"No historical snapshot available for {dataset_id}"
        )
    daily.sort(key=lambda item: (item.delivery.arrival_date, item.delivery.batch_id))
    return snapshots + daily
