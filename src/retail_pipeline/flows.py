"""Run the published historical snapshot pipeline from an S3 manifest URI."""

import json
import os
import subprocess
from contextlib import closing
from pathlib import Path
from typing import cast

import boto3
from prefect import flow, get_run_logger, task

from retail_pipeline.staging import (
    LoadedSnapshot,
    PublishedSnapshot,
    RedshiftClient,
    SnapshotSource,
    StagedSnapshotLoad,
    commit_snapshot_load,
    read_published_snapshot,
    stage_snapshot_for_load,
)


@task(name="read-published-manifest", persist_result=False)
def _read_manifest(s3: SnapshotSource, source_manifest_uri: str) -> PublishedSnapshot:
    published = read_published_snapshot(s3, source_manifest_uri)
    get_run_logger().info(
        "Read dataset %s, batch %s from %s",
        published.delivery.dataset_id,
        published.delivery.batch_id,
        source_manifest_uri,
    )
    return published


@task(name="stage-snapshot", persist_result=False)
def _stage(
    s3: SnapshotSource,
    redshift: RedshiftClient,
    published: PublishedSnapshot,
    *,
    workgroup: str,
    database: str,
    core_schema: str,
) -> StagedSnapshotLoad:
    staged = stage_snapshot_for_load(
        s3,
        redshift,
        published,
        workgroup=workgroup,
        database=database,
        core_schema=core_schema,
    )
    get_run_logger().info("Verified staged row counts: %s", staged.snapshot.row_counts)
    return staged


@task(name="load-core", persist_result=False)
def _load_core(redshift: RedshiftClient, staged: StagedSnapshotLoad) -> LoadedSnapshot:
    loaded = commit_snapshot_load(redshift, staged)
    get_run_logger().info("Loaded %s: %s", loaded.core_schema, loaded.row_counts)
    return loaded


@task(name="dbt-build", persist_result=False)
def _build_mart(core_schema: str, project_dir: Path) -> None:
    command = [
        "dbt",
        "--no-use-colors",
        "build",
        "--project-dir",
        str(project_dir),
        "--vars",
        json.dumps({"core_schema": core_schema}),
    ]
    logger = get_run_logger()
    with subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        errors="replace",
    ) as process:
        assert process.stdout is not None
        for line in process.stdout:
            if line.strip():
                logger.info("%s", line.rstrip())
        exit_code = process.wait()
    if exit_code:
        raise RuntimeError(f"dbt build failed with exit code {exit_code}")


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"Set {name} on the pipeline runner")
    return value


@flow(name="historical-snapshot", persist_result=False)
def run_snapshot(source_manifest_uri: str) -> LoadedSnapshot:
    """Stage, load, and build the mart for one published historical snapshot."""
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not region:
        raise ValueError("Set AWS_REGION or AWS_DEFAULT_REGION on the pipeline runner")
    workgroup = _required_env("RETAIL_REDSHIFT_WORKGROUP")
    database = _required_env("RETAIL_REDSHIFT_DATABASE")
    core_schema = _required_env("RETAIL_CORE_SCHEMA")
    project_dir = Path("dbt").resolve()
    if not (project_dir / "dbt_project.yml").is_file():
        raise FileNotFoundError("Run the snapshot flow from the repository root")

    session = boto3.Session(
        profile_name=os.environ.get("AWS_PROFILE"), region_name=region
    )
    with (
        closing(session.client("s3")) as s3_client,
        closing(session.client("redshift-data")) as redshift_client,
    ):
        s3 = cast(SnapshotSource, s3_client)
        redshift = cast(RedshiftClient, redshift_client)
        published = _read_manifest(s3, source_manifest_uri)
        staged = _stage(
            s3,
            redshift,
            published,
            workgroup=workgroup,
            database=database,
            core_schema=core_schema,
        )
        loaded = _load_core(redshift, staged)

    _build_mart(loaded.core_schema, project_dir)
    return loaded
