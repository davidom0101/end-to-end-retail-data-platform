"""Build the locked Redshift transaction for one staged incremental delivery."""

from retail_pipeline.classification import (
    ACCEPTED_DECISIONS,
    CLASSIFICATION_DECISIONS,
    classification_sql,
    classification_summary_sql,
    same_key_sql,
)
from retail_pipeline.discovery import PublishedDelivery
from retail_pipeline.manifest import SNAPSHOT_LAYOUTS

_REFERENCES = {
    "orders": (("customer_id", "customers"),),
    "order_items": (("order_id", "orders"), ("product_id", "products")),
}
_ACCEPTED = ", ".join(f"'{decision}'" for decision in ACCEPTED_DECISIONS)
_DECISIONS = ", ".join(f"'{decision}'" for decision in CLASSIFICATION_DECISIONS)
_READY = "(SELECT status FROM apply_status) = 'ready'"
_CAN_APPLY = f"{_READY} AND NOT EXISTS (SELECT 1 FROM apply_errors)"


def lock_core_sql(core_schema: str) -> str:
    """Acquire the ledger and core locks in the same order for every load."""
    tables = [f"{core_schema}.applied_batches"]
    tables.extend(f"{core_schema}.{entity}" for entity in SNAPSHOT_LAYOUTS)
    return f"LOCK TABLE {', '.join(tables)}"


def application_statements(
    core_schema: str,
    published: PublishedDelivery,
    counts: dict[str, int],
    *,
    fail_before_commit: bool = False,
) -> list[str]:
    """Build one transaction for a validated schema, manifest, and staged counts."""
    ledger = f"{core_schema}.applied_batches"
    dataset_id = published.delivery.dataset_id
    batch_id = published.delivery.batch_id
    checksum = published.manifest_sha256
    batch_key = f"dataset_id = '{dataset_id}' AND batch_id = '{batch_id}'"
    statements = [
        lock_core_sql(core_schema),
        f"""
CREATE TEMP TABLE apply_status AS
SELECT CASE
    WHEN NOT EXISTS (SELECT 1 FROM {ledger} WHERE kind = 'historical')
        THEN 'snapshot_required'
    WHEN NOT EXISTS (
        SELECT 1 FROM {ledger} WHERE kind = 'historical' AND dataset_id = '{dataset_id}'
    ) THEN 'wrong_dataset'
    WHEN EXISTS (
        SELECT 1 FROM {ledger} WHERE {batch_key}
        AND (kind <> 'incremental' OR manifest_sha256 <> '{checksum}')
    ) THEN 'changed_batch'
    WHEN EXISTS (SELECT 1 FROM {ledger} WHERE {batch_key}) THEN 'already_applied'
    ELSE 'ready'
END AS status
""".strip(),
    ]
    statements.extend(classification_sql(entity, core_schema) for entity in counts)
    statements.append(
        f"CREATE TEMP TABLE apply_summary AS {classification_summary_sql(counts)}"
    )
    statements.append(_validation_sql(core_schema, counts))
    for entity in SNAPSHOT_LAYOUTS:
        if entity in counts:
            statements.extend(_write_entity_sql(core_schema, entity))

    row_counts = ", ".join(str(counts.get(entity, 0)) for entity in SNAPSHOT_LAYOUTS)
    statements.append(
        f"INSERT INTO {ledger} (dataset_id, batch_id, kind, manifest_sha256, "
        "customer_rows, product_rows, order_rows, order_item_rows) "
        f"SELECT '{dataset_id}', '{batch_id}', 'incremental', '{checksum}', "
        f"{row_counts} WHERE {_CAN_APPLY}"
    )
    if fail_before_commit:
        # Fail a separate statement after the core and ledger inserts have run.
        statements.append(
            f"SELECT CAST(CASE WHEN {_CAN_APPLY} THEN 'injected-before-commit' "
            "ELSE '0' END AS INTEGER) FROM apply_status"
        )
    statements.append(
        "SELECT '_batch' AS entity, CASE "
        "WHEN status = 'ready' AND EXISTS (SELECT 1 FROM apply_errors) "
        "THEN 'rejected' WHEN status = 'ready' THEN 'applied' ELSE status END "
        "AS decision, CAST(0 AS BIGINT) AS versions, "
        "CAST(0 AS BIGINT) AS repeated, "
        "CAST('' AS VARCHAR(65535)) AS sample_key FROM apply_status "
        f"UNION ALL SELECT * FROM apply_errors WHERE {_READY} "
        f"UNION ALL SELECT * FROM apply_summary WHERE {_CAN_APPLY}"
    )
    return statements


def _validation_sql(core_schema: str, counts: dict[str, int]) -> str:
    checks = [f"SELECT * FROM apply_summary WHERE decision NOT IN ({_DECISIONS})"]
    for entity, expected in counts.items():
        checks.append(
            f"SELECT '{entity}', 'classification_count_mismatch', 0, 0, '' "
            "WHERE COALESCE((SELECT SUM(versions + repeated) FROM apply_summary "
            f"WHERE entity = '{entity}'), 0) <> {expected}"
        )
        key = SNAPSHOT_LAYOUTS[entity][0][0]
        for column, parent in _REFERENCES.get(entity, ()):
            parent_key = SNAPSHOT_LAYOUTS[parent][0][0]
            match = same_key_sql(f"parent.{parent_key}", f"child.{column}")
            missing = (
                f"NOT EXISTS (SELECT 1 FROM {core_schema}.{parent} parent "
                f"WHERE {match})"
            )
            if parent in counts:
                missing += (
                    f" AND NOT EXISTS (SELECT 1 FROM classified_{parent} parent "
                    f"WHERE parent.decision IN ({_ACCEPTED}) AND {match})"
                )
            checks.append(
                f"SELECT '{entity}', 'missing_{column}', COUNT(*), 0, MIN(child.{key}) "
                f"FROM classified_{entity} child "
                f"WHERE child.decision IN ({_ACCEPTED}) AND {missing} "
                "HAVING COUNT(*) > 0"
            )
    return "CREATE TEMP TABLE apply_errors AS " + " UNION ALL ".join(checks)


def _write_entity_sql(core_schema: str, entity: str) -> list[str]:
    key = SNAPSHOT_LAYOUTS[entity][0][0]
    names = [name for name, _ in SNAPSHOT_LAYOUTS[entity]]
    if entity == "customers":
        names.extend(("updated_at", "source_sequence"))
    if entity == "order_items":
        names.append("discount_amount")
    updates = ", ".join(f"{name} = incoming.{name}" for name in names if name != key)
    match = same_key_sql(f"{entity}.{key}", f"incoming.{key}")
    columns = ", ".join(names)
    return [
        f"UPDATE {core_schema}.{entity} SET {updates} "
        f"FROM classified_{entity} incoming "
        f"WHERE incoming.decision = 'update' AND {match} AND {_CAN_APPLY}",
        f"INSERT INTO {core_schema}.{entity} ({columns}) SELECT {columns} "
        f"FROM classified_{entity} WHERE decision = 'insert' AND {_CAN_APPLY}",
    ]
