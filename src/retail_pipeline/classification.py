"""Build warehouse queries that classify incremental rows by source version."""

from retail_pipeline.manifest import SNAPSHOT_LAYOUTS

_VERSION_NAMES = ("updated_at", "source_sequence")
ACCEPTED_DECISIONS = ("insert", "update")
CLASSIFICATION_DECISIONS = (*ACCEPTED_DECISIONS, "duplicate", "stale")


def classification_sql(
    entity: str, core_schema: str, *, validated: bool = False
) -> str:
    """Create a temporary relation with the payload and decision for each version."""
    key = SNAPSHOT_LAYOUTS[entity][0][0]
    payload = SNAPSHOT_LAYOUTS[entity][1:]
    payload = tuple(column for column in payload if column[0] not in _VERSION_NAMES)
    names = [key, *(name for name, _ in payload), *_VERSION_NAMES]
    groups = [*names, f"OCTET_LENGTH({key})"]
    groups.extend(f"OCTET_LENGTH({name})" for name, kind in payload if kind == "string")
    payload_matches = [_matching_column(name, kind) for name, kind in payload]
    selected = [f"incoming.{name}" for name in names]
    source_fields = names.copy()
    source = (
        f"validated_{entity} WHERE rejection_reason IS NULL"
        if validated
        else f"stage_{entity}"
    )
    if entity == "order_items":
        discount = "COALESCE(discount_amount, CAST(0 AS DECIMAL(12,2)))"
        source_fields.append(f"{discount} AS discount_amount")
        groups.append(discount)
        payload_matches.append(
            "COALESCE(current_row.discount_amount, 0) = incoming.discount_amount"
        )
        selected.append("incoming.discount_amount")

    return f"""
CREATE TEMP TABLE classified_{entity} AS
WITH versions AS (
    SELECT {", ".join(source_fields)}, COUNT(*) AS occurrences
    FROM {source}
    GROUP BY {", ".join(groups)}
), ranked AS (
    SELECT versions.*,
           COUNT(*) OVER (
               PARTITION BY {key}, OCTET_LENGTH({key}), updated_at, source_sequence
           ) AS payload_count,
           ROW_NUMBER() OVER (
               PARTITION BY {key}, OCTET_LENGTH({key})
               ORDER BY updated_at DESC, source_sequence DESC
           ) AS version_rank
    FROM versions
), current_state AS (
    SELECT {entity}.*,
           COUNT(*) OVER (PARTITION BY {key}, OCTET_LENGTH({key})) AS key_count
    FROM {core_schema}.{entity}
)
SELECT {", ".join(selected)}, incoming.occurrences,
       CASE
           WHEN incoming.updated_at IS NULL OR incoming.source_sequence IS NULL
                OR incoming.source_sequence < 0 THEN 'invalid_version'
           WHEN incoming.payload_count > 1 THEN 'conflict_in_delivery'
           WHEN current_row.key_count > 1 THEN 'duplicate_core_key'
           WHEN current_row.{key} IS NOT NULL AND (
               current_row.updated_at IS NULL OR current_row.source_sequence IS NULL
               OR current_row.source_sequence < 0
           ) THEN 'invalid_core_version'
           WHEN incoming.updated_at = current_row.updated_at
                AND incoming.source_sequence = current_row.source_sequence THEN
               CASE WHEN {" AND ".join(payload_matches)} THEN 'duplicate'
                    ELSE 'conflict_with_core' END
           WHEN incoming.version_rank > 1
                OR incoming.updated_at < current_row.updated_at
                OR (incoming.updated_at = current_row.updated_at
                    AND incoming.source_sequence < current_row.source_sequence)
                THEN 'stale'
           WHEN current_row.{key} IS NULL THEN 'insert'
           ELSE 'update'
       END AS decision
FROM ranked AS incoming
LEFT JOIN current_state AS current_row
  ON {same_key_sql(f"incoming.{key}", f"current_row.{key}")}
""".strip()


def same_key_sql(left: str, right: str) -> str:
    """Compare opaque IDs without losing trailing spaces in Redshift."""
    return f"{left} = {right} AND OCTET_LENGTH({left}) = OCTET_LENGTH({right})"


def _matching_column(name: str, kind: str) -> str:
    incoming = f"incoming.{name}"
    current = f"current_row.{name}"
    equal = f"{incoming} = {current}"
    # Redshift treats trailing spaces as equal in VARCHAR comparisons.
    if kind == "string":
        equal += f" AND OCTET_LENGTH({incoming}) = OCTET_LENGTH({current})"
    return f"COALESCE({equal}, {incoming} IS NULL AND {current} IS NULL)"


def classification_summary_sql(entities: dict[str, int]) -> str:
    """Count logical versions and extra copies, and report a sample key per decision."""
    queries = []
    for entity in entities:
        key = SNAPSHOT_LAYOUTS[entity][0][0]
        queries.append(
            f"SELECT '{entity}' AS entity, decision, COUNT(*) AS versions, "
            "CAST(SUM(occurrences - 1) AS BIGINT) AS repeated, "
            f"MIN({key}) AS sample_key "
            f"FROM classified_{entity} GROUP BY decision"
        )
    return " UNION ALL ".join(queries)
