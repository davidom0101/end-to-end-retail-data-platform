"""Validate one historical v1 delivery and return its verified file list."""

import json
import ntpath
import re
from dataclasses import dataclass
from datetime import date
from hashlib import file_digest, sha256
from pathlib import Path

import pyarrow.parquet as pq

_VERSION_COLUMNS = (("updated_at", "timestamp"), ("source_sequence", "int64"))
_SNAPSHOT_LAYOUTS = {
    "customers": (
        ("customer_id", "string"),
        ("created_at", "timestamp"),
        ("country", "string"),
        ("customer_segment", "string"),
    ),
    "products": (
        ("product_id", "string"),
        ("name", "string"),
        ("category", "string"),
        ("current_price", "money"),
        ("is_active", "bool"),
        *_VERSION_COLUMNS,
    ),
    "orders": (
        ("order_id", "string"),
        ("customer_id", "string"),
        ("order_date", "timestamp"),
        ("status", "string"),
        *_VERSION_COLUMNS,
    ),
    "order_items": (
        ("item_id", "string"),
        ("order_id", "string"),
        ("product_id", "string"),
        ("quantity", "int32"),
        ("unit_price", "money"),
        *_VERSION_COLUMNS,
    ),
}
_PARQUET_TYPES: dict[str, tuple[str, dict[str, object]]] = {
    "string": ("BYTE_ARRAY", {"Type": "String"}),
    "bool": ("BOOLEAN", {"Type": "None"}),
    "int32": ("INT32", {"Type": "None"}),
    "int64": ("INT64", {"Type": "None"}),
    "timestamp": (
        "INT64",
        {"Type": "Timestamp", "isAdjustedToUTC": True, "timeUnit": "microseconds"},
    ),
    "money": ("FIXED_LEN_BYTE_ARRAY", {"Type": "Decimal", "precision": 12, "scale": 2}),
}


class ManifestValidationError(ValueError):
    """The selected manifest or one of its source files violates the contract."""


@dataclass(frozen=True)
class SnapshotFile:
    path: Path
    relative_path: str
    entity_type: str
    row_count: int
    sha256: str


@dataclass(frozen=True)
class SnapshotManifest:
    path: Path
    dataset_id: str
    batch_id: str
    arrival_date: date
    sha256: str
    files: tuple[SnapshotFile, ...]


def validate_snapshot_manifest(path: Path) -> SnapshotManifest:
    """Read the manifest and verify every listed file without contacting AWS."""
    try:
        path = path.expanduser().resolve()
        raw = path.read_bytes()
        manifest: object = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ManifestValidationError(f"Cannot read manifest {path}: {exc}") from exc

    if not isinstance(manifest, dict):
        raise ManifestValidationError("Manifest must be a JSON object")
    dataset_id = manifest.get("dataset_id")
    batch_id = manifest.get("batch_id")
    for field, value in (("dataset_id", dataset_id), ("batch_id", batch_id)):
        if not isinstance(value, str) or not re.fullmatch(
            r"[a-z0-9][a-z0-9_-]*", value
        ):
            raise ManifestValidationError(f"Invalid {field}: {value!r}")
    if manifest.get("kind") != "historical":
        raise ManifestValidationError("Expected a historical snapshot manifest")
    arrival = manifest.get("arrival_date")
    if not isinstance(arrival, str) or not re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}", arrival
    ):
        raise ManifestValidationError(
            "arrival_date must be a UTC date in YYYY-MM-DD form"
        )
    try:
        arrival_date = date.fromisoformat(arrival)
    except ValueError as exc:
        raise ManifestValidationError(f"Invalid arrival_date: {arrival!r}") from exc

    entries = manifest.get("files")
    if not isinstance(entries, list) or not entries:
        raise ManifestValidationError("files must be a non-empty array")
    files: list[SnapshotFile] = []
    seen_paths: set[Path] = set()
    for entry in entries:
        file = _parse_file(entry, path.parent)
        if file.path in seen_paths:
            raise ManifestValidationError(f"Duplicate file path: {file.relative_path}")
        seen_paths.add(file.path)
        files.append(file)

    missing = _SNAPSHOT_LAYOUTS.keys() - {file.entity_type for file in files}
    if missing:
        raise ManifestValidationError(
            f"Missing snapshot entities: {', '.join(sorted(missing))}"
        )
    for file in files:
        _validate_parquet(file)
    return SnapshotManifest(
        path,
        str(dataset_id),
        str(batch_id),
        arrival_date,
        sha256(raw).hexdigest(),
        tuple(files),
    )


def _parse_file(entry: object, directory: Path) -> SnapshotFile:
    if not isinstance(entry, dict):
        raise ManifestValidationError("Each files entry must be a JSON object")
    relative = entry.get("path")
    if (
        not isinstance(relative, str)
        or not relative.endswith(".parquet")
        or "\\" in relative
        or ":" in relative
        or any(
            not part or part in {".", ".."} or ntpath.isreserved(part)
            for part in relative.split("/")
        )
    ):
        raise ManifestValidationError(f"Unsafe Parquet path: {relative!r}")
    try:
        path = (directory / relative).resolve()
    except OSError as exc:
        raise ManifestValidationError(f"Cannot resolve {relative}: {exc}") from exc
    if not path.is_relative_to(directory):
        raise ManifestValidationError(
            f"Parquet path leaves manifest directory: {relative}"
        )

    entity = entry.get("entity_type")
    if not isinstance(entity, str) or entity not in _SNAPSHOT_LAYOUTS:
        raise ManifestValidationError(f"{relative}: unknown entity_type {entity!r}")
    version = entry.get("schema_version")
    if type(version) is not int or version != 1:
        raise ManifestValidationError(
            f"{relative}: unsupported snapshot schema version {version!r}"
        )
    row_count = entry.get("row_count")
    if type(row_count) is not int or row_count <= 0:
        raise ManifestValidationError(
            f"{relative}: row_count must be a positive integer"
        )
    checksum = entry.get("sha256")
    if not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum):
        raise ManifestValidationError(f"{relative}: invalid SHA-256 digest")
    return SnapshotFile(path, relative, entity, row_count, checksum)


def _validate_parquet(file: SnapshotFile) -> None:
    try:
        with file.path.open("rb") as source:
            if file_digest(source, "sha256").hexdigest() != file.sha256:
                raise ManifestValidationError(f"{file.relative_path}: SHA-256 mismatch")
            source.seek(0)
            with pq.ParquetFile(source) as parquet:
                if parquet.metadata.num_rows != file.row_count:
                    raise ManifestValidationError(
                        f"{file.relative_path}: expected {file.row_count} rows, "
                        f"found {parquet.metadata.num_rows} in Parquet footer"
                    )
                _validate_layout(file, parquet)
    except ManifestValidationError:
        raise
    except (OSError, ValueError) as exc:
        raise ManifestValidationError(
            f"Cannot read {file.relative_path}: {exc}"
        ) from exc


def _validate_layout(file: SnapshotFile, parquet: pq.ParquetFile) -> None:
    schema = parquet.schema
    layout = _SNAPSHOT_LAYOUTS[file.entity_type]
    if schema.names != [name for name, _ in layout]:
        raise ManifestValidationError(
            f"{file.relative_path}: unexpected Parquet columns or order"
        )
    for index, (name, kind) in enumerate(layout):
        column = schema.column(index)
        physical, expected_logical = _PARQUET_TYPES[kind]
        logical = json.loads(column.logical_type.to_json())
        if (
            column.path != name
            or column.max_definition_level != 1
            or column.max_repetition_level != 0
            or column.physical_type != physical
            or any(logical.get(key) != value for key, value in expected_logical.items())
            or (kind == "money" and column.length != 6)
        ):
            raise ManifestValidationError(
                f"{file.relative_path}: incompatible Parquet column {name!r}"
            )
    for index in range(parquet.metadata.num_row_groups):
        group = parquet.metadata.row_group(index)
        if any(
            group.column(i).compression != "SNAPPY" for i in range(group.num_columns)
        ):
            raise ManifestValidationError(
                f"{file.relative_path}: expected Snappy compression"
            )
