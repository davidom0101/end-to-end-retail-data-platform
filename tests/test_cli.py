from pathlib import Path

import pytest

from retail_pipeline.cli import parse_settings


def test_cli_overrides_environment_and_uses_aws_region_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_path = Path(__file__).resolve().parent
    monkeypatch.setenv("RETAIL_DATASET_PATH", str(dataset_path / "unused"))
    monkeypatch.setenv("RETAIL_RAW_BUCKET", "raw-from-environment")
    monkeypatch.setenv("RETAIL_REDSHIFT_WORKGROUP", "retail-dev")
    monkeypatch.setenv("RETAIL_REDSHIFT_DATABASE", "retail")
    monkeypatch.setenv("AWS_PROFILE", "local-dev")
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")

    settings = parse_settings(
        ["--dataset-path", str(dataset_path), "--raw-bucket", "raw-cli"]
    )

    assert settings.dataset_path == dataset_path
    assert settings.raw_bucket == "raw-cli"
    assert settings.aws_profile == "local-dev"
    assert settings.aws_region == "eu-west-1"
    assert settings.redshift_workgroup == "retail-dev"
    assert settings.redshift_database == "retail"
