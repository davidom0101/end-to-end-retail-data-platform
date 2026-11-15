"""Build row checks and retain rejected incremental events for investigation."""

from retail_pipeline.classification import ACCEPTED_DECISIONS
from retail_pipeline.manifest import (
    DISCOUNT_COLUMN,
    INCREMENTAL_LAYOUTS,
    SNAPSHOT_LAYOUTS,
)

_REFERENCES = {
    "orders": (("customer_id", "customers", "customer"),),
    "order_items": (
        ("order_id", "orders", "order"),
        ("product_id", "products", "product"),
    ),
}
_BUSINESS_RULES = {
    "customers": (),
    "products": (("current_price < 0", "Catalog price is negative"),),
    "orders": (
        (
            "status NOT IN ('pending', 'paid', 'shipped', 'delivered', 'canceled') "
            "OR OCTET_LENGTH(status) <> OCTET_LENGTH(TRIM(status))",
            "Invalid order status",
        ),
    ),
    "order_items": (
        ("quantity <= 0", "Quantity must be positive"),
        ("unit_price < 0", "Unit price is negative"),
        ("discount_amount < 0", "Discount is negative"),
        (
            "discount_amount > quantity * unit_price",
            "Discount exceeds gross line amount",
        ),
    ),
}


def validate_rows_sql(entity: str, core_schema: str, entities: dict[str, int]) -> str:
    """Mark the first failure per source row before choosing current versions."""
    checks = []
    for field, kind in INCREMENTAL_LAYOUTS[entity]:
        missing = f"incoming.{field} IS NULL"
        if kind == "string":
            missing += f" OR TRIM(incoming.{field}) = ''"
        checks.append((missing, f"Missing {field}"))
    checks.extend(
        [
            ("operation NOT IN ('insert', 'update')", "Invalid event operation"),
            ("source_sequence < 0", "Negative source sequence"),
            *_BUSINESS_RULES[entity],
        ]
    )
    accepted = ", ".join(f"'{decision}'" for decision in ACCEPTED_DECISIONS)
    joins = []
    for field, parent, label in _REFERENCES.get(entity, ()):
        parent_key = SNAPSHOT_LAYOUTS[parent][0][0]
        parent_alias = f"accepted_{label}"
        keys = f"{parent_key}, OCTET_LENGTH({parent_key}) AS key_bytes"
        eligible = f"SELECT DISTINCT {keys} FROM {core_schema}.{parent}"
        if parent in entities:
            eligible += (
                f" UNION SELECT {keys} FROM classified_{parent} "
                f"WHERE decision IN ({accepted})"
            )
        joins.append(
            f"LEFT JOIN ({eligible}) {parent_alias} "
            f"ON {parent_alias}.{parent_key} = incoming.{field} "
            f"AND {parent_alias}.key_bytes = OCTET_LENGTH(incoming.{field})"
        )
        checks.append(
            (
                f"{parent_alias}.{parent_key} IS NULL",
                f"Referenced {label} does not exist",
            )
        )

    cases = "\n".join(
        f"    WHEN {condition} THEN '{reason}'" for condition, reason in checks
    )
    return (
        f"CREATE TEMP TABLE validated_{entity} AS\n"
        f"SELECT incoming.*, CASE\n{cases}\nEND AS rejection_reason\n"
        f"FROM stage_{entity} incoming\n" + "\n".join(joins)
    )


def quarantine_insert_sql(
    schema: str,
    entity: str,
    dataset_id: str,
    batch_id: str,
    can_apply: str,
    *,
    has_discount: bool,
) -> str:
    """Preserve the physical source fields, including null values, as SUPER."""
    columns = INCREMENTAL_LAYOUTS[entity]
    if has_discount:
        columns = (*columns, DISCOUNT_COLUMN)
    fields = []
    for name, kind in columns:
        value = f"CAST({name} AS VARCHAR)" if kind == "timestamp" else name
        fields.append(f"'{name}', {value}")
    return (
        f"INSERT INTO {schema}.quarantined_events "
        "(dataset_id, batch_id, entity_type, event_id, record_id, reason, "
        "source_payload) "
        f"SELECT '{dataset_id}', '{batch_id}', '{entity}', event_id, record_id, "
        f"rejection_reason, OBJECT({', '.join(fields)}) FROM validated_{entity} "
        f"WHERE rejection_reason IS NOT NULL AND {can_apply}"
    )
