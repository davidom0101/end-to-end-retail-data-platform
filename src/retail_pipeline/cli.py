import argparse
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path


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
        description="Display the pipeline configuration without connecting to AWS."
    )
    options = (
        ("--dataset-path", "RETAIL_DATASET_PATH", "Local dataset directory"),
        ("--aws-profile", "AWS_PROFILE", "Named AWS profile (optional)"),
        ("--aws-region", "AWS_REGION", "AWS Region"),
        ("--raw-bucket", "RETAIL_RAW_BUCKET", "Raw S3 bucket name"),
        ("--redshift-workgroup", "RETAIL_REDSHIFT_WORKGROUP", "Redshift workgroup"),
        ("--redshift-database", "RETAIL_REDSHIFT_DATABASE", "Redshift database"),
    )
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


def main() -> None:
    settings = parse_settings()
    output = asdict(settings)
    output["dataset_path"] = str(settings.dataset_path)
    print(json.dumps(output, indent=2))
