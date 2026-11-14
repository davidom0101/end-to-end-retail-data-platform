import argparse
import json
import os
import re
import sys
from collections import Counter
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import cast

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from retail_pipeline.discovery import (
    DeliverySelectionError,
    DeliverySource,
    SnapshotSource,
    discover_deliveries,
    read_published_delivery,
)
from retail_pipeline.manifest import ManifestValidationError, validate_delivery_manifest
from retail_pipeline.staging import (
    RedshiftClient,
    SnapshotStagingError,
    load_snapshot,
    stage_snapshot,
)
from retail_pipeline.upload import S3Client, SnapshotUploadError, upload_delivery

_S3_OPTIONS = (
    ("--aws-profile", "AWS_PROFILE", "Named AWS profile (optional)"),
    ("--aws-region", "AWS_REGION", "AWS Region"),
    ("--raw-bucket", "RETAIL_RAW_BUCKET", "Raw S3 bucket name"),
)
_WAREHOUSE_OPTIONS = (
    ("--redshift-workgroup", "RETAIL_REDSHIFT_WORKGROUP", "Redshift workgroup"),
    ("--redshift-database", "RETAIL_REDSHIFT_DATABASE", "Redshift database"),
)


@dataclass(frozen=True)
class PipelineSettings:
    dataset_path: Path
    aws_profile: str | None
    aws_region: str
    raw_bucket: str
    redshift_workgroup: str
    redshift_database: str


def parse_settings(argv: list[str] | None = None) -> PipelineSettings:
    """Read pipeline settings, with CLI values overriding environment values."""
    parser = argparse.ArgumentParser(
        description="Display the pipeline configuration without connecting to AWS.",
        epilog="Commands: validate-manifest, upload-delivery, upload-snapshot, "
        "stage-snapshot, load-snapshot, run-snapshot, discover-deliveries, "
        "stage-incremental.",
    )
    options = (
        ("--dataset-path", "RETAIL_DATASET_PATH", "Local dataset directory"),
        *_S3_OPTIONS,
        *_WAREHOUSE_OPTIONS,
    )
    _add_options(parser, options)
    args = parser.parse_args(argv)
    dataset_path = Path(args.dataset_path).expanduser().resolve()
    if not dataset_path.is_dir():
        parser.error(f"dataset directory does not exist: {dataset_path}")

    return PipelineSettings(
        dataset_path=dataset_path,
        aws_profile=args.aws_profile,
        aws_region=args.aws_region,
        raw_bucket=args.raw_bucket,
        redshift_workgroup=args.redshift_workgroup,
        redshift_database=args.redshift_database,
    )


def _add_options(
    parser: argparse.ArgumentParser, options: tuple[tuple[str, str, str], ...]
) -> None:
    for flag, variable, description in options:
        value = os.environ.get(variable)
        if variable == "AWS_REGION":
            value = value or os.environ.get("AWS_DEFAULT_REGION")
        parser.add_argument(
            flag,
            default=value or None,
            required=not value and variable != "AWS_PROFILE",
            help=f"{description}. Environment: {variable}.",
        )


def main() -> None:
    arguments = sys.argv[1:]
    if arguments[:1] == ["validate-manifest"]:
        _validate_manifest_command(arguments[1:])
        return
    if arguments[:1] == ["upload-snapshot"]:
        _upload_delivery_command(arguments[1:], historical_only=True)
        return
    if arguments[:1] == ["upload-delivery"]:
        _upload_delivery_command(arguments[1:])
        return
    if arguments[:1] == ["stage-snapshot"]:
        _stage_snapshot_command(arguments[1:])
        return
    if arguments[:1] == ["load-snapshot"]:
        _stage_snapshot_command(arguments[1:], load_core=True)
        return
    if arguments[:1] == ["run-snapshot"]:
        _run_snapshot_command(arguments[1:])
        return
    if arguments[:1] == ["discover-deliveries"]:
        _discover_deliveries_command(arguments[1:])
        return
    if arguments[:1] == ["stage-incremental"]:
        _stage_incremental_command(arguments[1:])
        return

    settings = parse_settings(arguments)
    output = asdict(settings)
    output["dataset_path"] = str(settings.dataset_path)
    print(json.dumps(output, indent=2))


def _validate_manifest_command(arguments: list[str]) -> None:
    parser = argparse.ArgumentParser(
        prog="retail-pipeline validate-manifest",
        description="Verify one historical or incremental v1 delivery locally.",
    )
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args(arguments)
    try:
        snapshot = validate_delivery_manifest(args.manifest)
    except ManifestValidationError as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")

    counts: Counter[str] = Counter()
    for file in snapshot.files:
        counts[file.entity_type] += file.row_count
    print(
        json.dumps(
            {
                "dataset_id": snapshot.dataset_id,
                "batch_id": snapshot.batch_id,
                "manifest_sha256": snapshot.sha256,
                "file_count": len(snapshot.files),
                "row_counts": counts,
            },
            indent=2,
        )
    )


def _upload_delivery_command(
    arguments: list[str], *, historical_only: bool = False
) -> None:
    command = "upload-snapshot" if historical_only else "upload-delivery"
    source = (
        "historical v1 snapshot"
        if historical_only
        else "historical or incremental v1 delivery"
    )
    parser = argparse.ArgumentParser(
        prog=f"retail-pipeline {command}",
        description=f"Validate and upload one {source} to S3.",
    )
    parser.add_argument("manifest", type=Path)
    _add_options(parser, _S3_OPTIONS)
    args = parser.parse_args(arguments)
    try:
        delivery = validate_delivery_manifest(
            args.manifest, historical_only=historical_only
        )
        session = boto3.Session(
            profile_name=args.aws_profile, region_name=args.aws_region
        )
        with closing(session.client("s3")) as client:
            published = upload_delivery(
                cast(S3Client, client), args.raw_bucket, delivery
            )
    except (
        ManifestValidationError,
        SnapshotUploadError,
        ClientError,
        BotoCoreError,
        OSError,
    ) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")
    print(json.dumps(asdict(published), indent=2))


def _stage_snapshot_command(arguments: list[str], *, load_core: bool = False) -> None:
    command = "load-snapshot" if load_core else "stage-snapshot"
    parser = argparse.ArgumentParser(
        prog=f"retail-pipeline {command}",
        description=(
            "Replace the selected demo tables with a published historical snapshot."
            if load_core
            else "Verify a published historical snapshot through Redshift staging."
        ),
    )
    parser.add_argument("source_manifest_uri")
    if load_core:
        parser.add_argument(
            "--core-schema", required=True, help="core_demo or core_demo_<name>"
        )
    _add_options(parser, (*_S3_OPTIONS[:2], *_WAREHOUSE_OPTIONS))
    args = parser.parse_args(arguments)
    try:
        session = boto3.Session(
            profile_name=args.aws_profile, region_name=args.aws_region
        )
        with (
            closing(session.client("s3")) as s3,
            closing(session.client("redshift-data")) as redshift,
        ):
            source = cast(SnapshotSource, s3)
            warehouse = cast(RedshiftClient, redshift)
            connection = {
                "workgroup": args.redshift_workgroup,
                "database": args.redshift_database,
            }
            if load_core:
                result = asdict(
                    load_snapshot(
                        source,
                        warehouse,
                        args.source_manifest_uri,
                        core_schema=args.core_schema,
                        **connection,
                    )
                )
            else:
                result = asdict(
                    stage_snapshot(
                        source,
                        warehouse,
                        args.source_manifest_uri,
                        **connection,
                    )
                )
    except (
        ManifestValidationError,
        SnapshotStagingError,
        ClientError,
        BotoCoreError,
    ) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")
    print(json.dumps(result, indent=2))


def _run_snapshot_command(arguments: list[str]) -> None:
    parser = argparse.ArgumentParser(
        prog="retail-pipeline run-snapshot",
        description="Run the Prefect flow for a published S3 snapshot manifest.",
    )
    parser.add_argument("source_manifest_uri")
    args = parser.parse_args(arguments)

    from retail_pipeline.flows import run_snapshot

    run_snapshot(args.source_manifest_uri)


def _stage_incremental_command(arguments: list[str]) -> None:
    parser = argparse.ArgumentParser(
        prog="retail-pipeline stage-incremental",
        description="Verify a published incremental v1 delivery in temporary staging.",
    )
    parser.add_argument("source_manifest_uri")
    args = parser.parse_args(arguments)

    from retail_pipeline.flows import run_incremental_staging

    counts = run_incremental_staging(args.source_manifest_uri)
    print(json.dumps(counts, indent=2))


def _parse_cutoff(value: str) -> date:
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                f"Invalid UTC cutoff date: {value}"
            ) from exc
    raise argparse.ArgumentTypeError("Cutoff must be a UTC date in YYYY-MM-DD form")


def _discover_deliveries_command(arguments: list[str]) -> None:
    parser = argparse.ArgumentParser(
        prog="retail-pipeline discover-deliveries",
        description="Read one published manifest or list normal deliveries in order.",
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("source_manifest_uri", nargs="?")
    selection.add_argument("--through", type=_parse_cutoff, metavar="YYYY-MM-DD")
    parser.add_argument(
        "--dataset-id", help="Dataset release to discover with --through"
    )
    _add_options(parser, _S3_OPTIONS[:2])
    parser.add_argument("--raw-bucket", default=os.environ.get("RETAIL_RAW_BUCKET"))
    args = parser.parse_args(arguments)
    if args.through and (not args.dataset_id or not args.raw_bucket):
        parser.error("--through requires --dataset-id and a raw bucket")
    if args.source_manifest_uri and args.dataset_id:
        parser.error("--dataset-id is only used with --through")

    try:
        session = boto3.Session(
            profile_name=args.aws_profile, region_name=args.aws_region
        )
        with closing(session.client("s3")) as client:
            source = cast(DeliverySource, client)
            if args.source_manifest_uri:
                deliveries = [read_published_delivery(source, args.source_manifest_uri)]
            else:
                deliveries = discover_deliveries(
                    source,
                    bucket=args.raw_bucket,
                    dataset_id=args.dataset_id,
                    cutoff=args.through,
                )
    except (
        DeliverySelectionError,
        ManifestValidationError,
        ClientError,
        BotoCoreError,
    ) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")

    print(
        json.dumps(
            [
                {
                    "source_manifest_uri": published.source_manifest_uri,
                    "dataset_id": published.delivery.dataset_id,
                    "batch_id": published.delivery.batch_id,
                    "kind": published.delivery.kind,
                    "arrival_date": published.delivery.arrival_date.isoformat(),
                }
                for published in deliveries
            ],
            indent=2,
        )
    )
