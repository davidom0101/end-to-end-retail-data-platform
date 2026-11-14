import argparse
import json
import os
import sys
from collections import Counter
from contextlib import closing
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import cast

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from retail_pipeline.manifest import ManifestValidationError, validate_snapshot_manifest
from retail_pipeline.staging import (
    RedshiftClient,
    SnapshotSource,
    SnapshotStagingError,
    stage_snapshot,
)
from retail_pipeline.upload import S3Client, SnapshotUploadError, upload_snapshot

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
        epilog="Snapshot commands: validate-manifest, upload-snapshot, stage-snapshot.",
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
        _upload_snapshot_command(arguments[1:])
        return
    if arguments[:1] == ["stage-snapshot"]:
        _stage_snapshot_command(arguments[1:])
        return

    settings = parse_settings(arguments)
    output = asdict(settings)
    output["dataset_path"] = str(settings.dataset_path)
    print(json.dumps(output, indent=2))


def _validate_manifest_command(arguments: list[str]) -> None:
    parser = argparse.ArgumentParser(
        prog="retail-pipeline validate-manifest",
        description="Verify one historical v1 snapshot locally.",
    )
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args(arguments)
    try:
        snapshot = validate_snapshot_manifest(args.manifest)
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


def _upload_snapshot_command(arguments: list[str]) -> None:
    parser = argparse.ArgumentParser(
        prog="retail-pipeline upload-snapshot",
        description="Validate and upload one historical v1 snapshot to S3.",
    )
    parser.add_argument("manifest", type=Path)
    _add_options(parser, _S3_OPTIONS)
    args = parser.parse_args(arguments)
    try:
        snapshot = validate_snapshot_manifest(args.manifest)
        session = boto3.Session(
            profile_name=args.aws_profile, region_name=args.aws_region
        )
        with closing(session.client("s3")) as client:
            published = upload_snapshot(
                cast(S3Client, client), args.raw_bucket, snapshot
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


def _stage_snapshot_command(arguments: list[str]) -> None:
    parser = argparse.ArgumentParser(
        prog="retail-pipeline stage-snapshot",
        description="Verify a published historical snapshot through Redshift staging.",
    )
    parser.add_argument("source_manifest_uri")
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
            staged = stage_snapshot(
                cast(SnapshotSource, s3),
                cast(RedshiftClient, redshift),
                args.source_manifest_uri,
                workgroup=args.redshift_workgroup,
                database=args.redshift_database,
            )
    except (
        ManifestValidationError,
        SnapshotStagingError,
        ClientError,
        BotoCoreError,
    ) as exc:
        parser.exit(1, f"{parser.prog}: {exc}\n")
    print(json.dumps(asdict(staged), indent=2))
