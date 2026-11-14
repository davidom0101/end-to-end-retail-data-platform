import json
import sys
from contextlib import closing
from datetime import date
from io import BytesIO

import boto3
import pytest
from botocore.stub import Stubber

from retail_pipeline import cli
from retail_pipeline.discovery import (
    DeliverySelectionError,
    discover_deliveries,
    read_published_delivery,
)

BUCKET = "retail-raw"
PREFIX = "raw/demo-v1/"


def _manifest(batch_id: str, arrival: str, *, historical: bool = False) -> bytes:
    entities = (
        ("customers", "products", "orders", "order_items")
        if historical
        else ("order_items",)
    )
    return json.dumps(
        {
            "dataset_id": "demo-v1",
            "batch_id": batch_id,
            "kind": "historical" if historical else "incremental",
            "arrival_date": arrival,
            "files": [
                {
                    "path": f"{entity}/part-001.parquet",
                    "entity_type": entity,
                    "schema_version": 1,
                    "row_count": 1,
                    "sha256": "a" * 64,
                }
                for entity in entities
            ],
        }
    ).encode()


def _key(batch_id: str) -> str:
    return f"{PREFIX}{batch_id}/manifest.json"


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch):
    session = boto3.Session(
        aws_access_key_id="testing", aws_secret_access_key="testing"
    )
    with (
        closing(session.client("s3", region_name="eu-west-1")) as client,
        Stubber(client) as stub,
    ):
        monkeypatch.setattr(cli.boto3, "Session", lambda **_: session)
        monkeypatch.setattr(session, "client", lambda name: client)
        monkeypatch.setenv("AWS_REGION", "eu-west-1")
        monkeypatch.setenv("RETAIL_RAW_BUCKET", BUCKET)
        yield client, stub
        stub.assert_no_pending_responses()


def _add_manifest(
    stub: Stubber, batch_id: str, arrival: str, *, historical: bool = False
):
    stub.add_response(
        "get_object",
        {"Body": BytesIO(_manifest(batch_id, arrival, historical=historical))},
        {"Bucket": BUCKET, "Key": _key(batch_id)},
    )


def test_discover_orders_across_pages_and_excludes_future_and_scenarios(
    s3, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _, stub = s3
    stub.add_response(
        "list_objects_v2",
        {
            "Contents": [
                {"Key": _key("daily-003")},
                {"Key": _key("daily-004")},
                {"Key": _key("duplicate-001")},
                {"Key": f"{PREFIX}daily-005/copy/orders.json"},
            ],
            "IsTruncated": True,
            "NextContinuationToken": "next",
        },
        {"Bucket": BUCKET, "Prefix": PREFIX},
    )
    _add_manifest(stub, "daily-003", "2026-11-12")
    _add_manifest(stub, "daily-004", "2026-11-13")
    stub.add_response(
        "list_objects_v2",
        {
            "Contents": [
                {"Key": _key("daily-001")},
                {"Key": _key("daily-002")},
                {"Key": _key("snapshot-001")},
            ],
            "IsTruncated": False,
        },
        {"Bucket": BUCKET, "Prefix": PREFIX, "ContinuationToken": "next"},
    )
    _add_manifest(stub, "daily-001", "2026-11-12")
    _add_manifest(stub, "daily-002", "2026-11-11")
    _add_manifest(stub, "snapshot-001", "2026-11-10", historical=True)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "retail-pipeline",
            "discover-deliveries",
            "--dataset-id",
            "demo-v1",
            "--through",
            "2026-11-12",
        ],
    )
    cli.main()

    result = json.loads(capsys.readouterr().out)
    assert [item["batch_id"] for item in result] == [
        "snapshot-001",
        "daily-002",
        "daily-001",
        "daily-003",
    ]


def test_explicit_manifest_can_select_a_scenario(
    s3, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _, stub = s3
    _add_manifest(stub, "late-001", "2026-11-13")
    uri = f"s3://{BUCKET}/{_key('late-001')}"
    monkeypatch.setattr(sys, "argv", ["retail-pipeline", "discover-deliveries", uri])

    cli.main()

    assert json.loads(capsys.readouterr().out) == [
        {
            "source_manifest_uri": uri,
            "dataset_id": "demo-v1",
            "batch_id": "late-001",
            "kind": "incremental",
            "arrival_date": "2026-11-13",
        }
    ]


def test_discover_requires_snapshot_when_daily_deliveries_are_available(s3) -> None:
    client, stub = s3
    stub.add_response(
        "list_objects_v2",
        {"Contents": [{"Key": _key("daily-001")}]},
        {"Bucket": BUCKET, "Prefix": PREFIX},
    )
    _add_manifest(stub, "daily-001", "2026-11-12")

    with pytest.raises(
        DeliverySelectionError, match="No historical snapshot available"
    ):
        discover_deliveries(
            client, bucket=BUCKET, dataset_id="demo-v1", cutoff=date(2026, 11, 12)
        )


def test_explicit_manifest_rejects_a_batch_id_different_from_the_key(s3) -> None:
    client, stub = s3
    stub.add_response(
        "get_object",
        {"Body": BytesIO(_manifest("late-002", "2026-11-13"))},
        {"Bucket": BUCKET, "Key": _key("late-001")},
    )

    with pytest.raises(DeliverySelectionError, match="Manifest IDs do not match"):
        read_published_delivery(client, f"s3://{BUCKET}/{_key('late-001')}")
