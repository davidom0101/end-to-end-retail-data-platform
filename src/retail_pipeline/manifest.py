"""Read delivery manifests and verify local v1 Parquet files."""

import json
import ntpath
import re
from dataclasses import dataclass
from datetime import date
from hashlib import file_digest, sha256
from pathlib import Path

import pyarrow.parquet as pq

_VERSION_COLUMNS = (("updated_at", "timestamp"), ("source_sequence", "int64"))
BATCH_ID_PATTERN = r"[a-z0-9][a-z0-9_-]*"
SNAPSHOT_LAYOUTS = {
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
_EVENT_COLUMNS = (
    ("event_id", "string"),
    ("entity_type", "string"),
    ("record_id", "string"),
    ("operation", "string"),
    ("event_time", "timestamp"),
    *_VERSION_COLUMNS,
    ("arrival_at", "timestamp"),
)
INCREMENTAL_LAYOUTS = {
    entity: (
        *_EVENT_COLUMNS,
        *(column for column in columns if column not in _VERSION_COLUMNS),
    )
    for entity, columns in SNAPSHOT_LAYOUTS.items()
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


@dataclass(frozen=True)
class BatchFileEntry:
    relative_path: str
    entity_type: str
    row_count: int
    sha256: str
    schema_version: int


@dataclass(frozen=True)
class BatchDelivery:
    dataset_id: str
    batch_id: str
    kind: str
    arrival_date: date
    files: tuple[BatchFileEntry, ...]


def validate_snapshot_manifest(path: Path) -> SnapshotManifest:
    """Verify a local historical v1 snapshot."""
    return validate_delivery_manifest(path, historical_only=True)


def validate_delivery_manifest(
    path: Path, *, historical_only: bool = False
) -> SnapshotManifest:
    """Verify all local Parquet files in a historical or incremental v1 delivery."""
    try:
        path = path.expanduser().resolve()
        raw = path.read_bytes()
    except OSError as exc:
        raise ManifestValidationError(f"Cannot read manifest {path}: {exc}") from exc
    delivery = parse_batch_manifest(raw, historical_only=historical_only)
    layouts = v1_layouts(delivery)
    files = []
    seen_paths: set[Path] = set()
    for entry in delivery.files:
        try:
            file_path = (path.parent / entry.relative_path).resolve()
        except OSError as exc:
            raise ManifestValidationError(
                f"Cannot resolve {entry.relative_path}: {exc}"
            ) from exc
        if not file_path.is_relative_to(path.parent):
            raise ManifestValidationError(
                f"Parquet path leaves manifest directory: {entry.relative_path}"
            )
        if file_path in seen_paths:
            raise ManifestValidationError(f"Duplicate file path: {entry.relative_path}")
        seen_paths.add(file_path)
        file = SnapshotFile(
            file_path,
            entry.relative_path,
            entry.entity_type,
            entry.row_count,
            entry.sha256,
        )
        _validate_parquet(file, layouts[file.entity_type])
        files.append(file)
    return SnapshotManifest(
        path,
        delivery.dataset_id,
        delivery.batch_id,
        delivery.arrival_date,
        sha256(raw).hexdigest(),
        tuple(files),
    )


def parse_snapshot_manifest(raw: bytes) -> BatchDelivery:
    """Validate historical v1 manifest metadata independently of file storage."""
    return parse_batch_manifest(raw, historical_only=True)


def parse_batch_manifest(raw: bytes, *, historical_only: bool = False) -> BatchDelivery:
    """Validate published manifest metadata for a historical or incremental batch."""
    try:
        manifest: object = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ManifestValidationError(f"Invalid manifest JSON: {exc}") from exc
    if not isinstance(manifest, dict):
        raise ManifestValidationError("Manifest must be a JSON object")
    dataset_id = manifest.get("dataset_id")
    batch_id = manifest.get("batch_id")
    for field, value in (("dataset_id", dataset_id), ("batch_id", batch_id)):
        if not isinstance(value, str) or not re.fullmatch(BATCH_ID_PATTERN, value):
            raise ManifestValidationError(f"Invalid {field}: {value!r}")
    kind = manifest.get("kind")
    if historical_only and kind != "historical":
        raise ManifestValidationError("Expected a historical snapshot manifest")
    if kind not in ("historical", "incremental"):
        raise ManifestValidationError(f"Invalid kind: {kind!r}")
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
    files = tuple(_parse_file(entry, kind=str(kind)) for entry in entries)
    if len({file.relative_path for file in files}) != len(files):
        raise ManifestValidationError("Duplicate file path in manifest")

    versions: dict[str, int] = {}
    for file in files:
        if (
            versions.setdefault(file.entity_type, file.schema_version)
            != file.schema_version
        ):
            raise ManifestValidationError(
                f"Mixed schema versions for {file.entity_type}"
            )
    if kind == "historical":
        missing = SNAPSHOT_LAYOUTS.keys() - versions.keys()
        if missing:
            raise ManifestValidationError(
                f"Missing snapshot entities: {', '.join(sorted(missing))}"
            )
    return BatchDelivery(str(dataset_id), str(batch_id), str(kind), arrival_date, files)


def _parse_file(entry: object, *, kind: str) -> BatchFileEntry:
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
    entity = entry.get("entity_type")
    if not isinstance(entity, str) or entity not in SNAPSHOT_LAYOUTS:
        raise ManifestValidationError(f"{relative}: unknown entity_type {entity!r}")
    version = entry.get("schema_version")
    allowed_versions = (
        (1, 2) if kind == "incremental" and entity == "order_items" else (1,)
    )
    if type(version) is not int or version not in allowed_versions:
        label = "snapshot" if kind == "historical" else "incremental"
        raise ManifestValidationError(
            f"{relative}: unsupported {label} schema version {version!r}"
        )
    row_count = entry.get("row_count")
    if type(row_count) is not int or row_count <= 0:
        raise ManifestValidationError(
            f"{relative}: row_count must be a positive integer"
        )
    checksum = entry.get("sha256")
    if not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum):
        raise ManifestValidationError(f"{relative}: invalid SHA-256 digest")
    return BatchFileEntry(relative, entity, row_count, checksum, version)


def v1_layouts(delivery: BatchDelivery) -> dict[str, tuple[tuple[str, str], ...]]:
    """Return the supported v1 column order or reject a newer delivery version."""
    for file in delivery.files:
        if file.schema_version != 1:
            raise ManifestValidationError(
                f"{file.relative_path}: unsupported {delivery.kind} schema "
                f"version {file.schema_version}"
            )
    return SNAPSHOT_LAYOUTS if delivery.kind == "historical" else INCREMENTAL_LAYOUTS


def _validate_parquet(file: SnapshotFile, layout: tuple[tuple[str, str], ...]) -> None:
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
                _validate_layout(file, parquet, layout)
    except ManifestValidationError:
        raise
    except (OSError, ValueError) as exc:
        raise ManifestValidationError(
            f"Cannot read {file.relative_path}: {exc}"
        ) from exc


def _validate_layout(
    file: SnapshotFile, parquet: pq.ParquetFile, layout: tuple[tuple[str, str], ...]
) -> None:
    schema = parquet.schema
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
