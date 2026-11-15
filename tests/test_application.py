import sqlite3
from collections.abc import Iterator
from contextlib import closing
from datetime import date

import pytest

from retail_pipeline.application import application_statements
from retail_pipeline.discovery import PublishedDelivery
from retail_pipeline.manifest import SNAPSHOT_LAYOUTS, BatchDelivery

CORE = "core_demo"


@pytest.fixture
def warehouse() -> Iterator[sqlite3.Connection]:
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.create_function(
            "OCTET_LENGTH",
            1,
            lambda value: len(value.encode()) if value is not None else None,
        )
        connection.execute("ATTACH ':memory:' AS core_demo")
        for entity, layout in SNAPSHOT_LAYOUTS.items():
            columns = [name for name, _ in layout]
            if entity == "customers":
                columns.extend(("updated_at", "source_sequence"))
            if entity == "order_items":
                columns.append("discount_amount")
            connection.execute(f"CREATE TABLE stage_{entity} ({', '.join(columns)})")
            connection.execute(f"CREATE TABLE {CORE}.{entity} ({', '.join(columns)})")
        connection.execute(f"""
            CREATE TABLE {CORE}.applied_batches (
              dataset_id, batch_id, kind, manifest_sha256, customer_rows, product_rows,
              order_rows, order_item_rows
            )
        """)
        connection.execute(f"""
            INSERT INTO {CORE}.applied_batches
            VALUES ('demo-v1', 'snapshot-001', 'historical', 'hash', 1, 1, 1, 1)
        """)
        connection.execute(f"""
            INSERT INTO {CORE}.products
            VALUES ('P0', 'Notebook', 'Stationery', 10, 1, '2026-01-01', 0)
        """)
        connection.execute(f"""
            INSERT INTO {CORE}.order_items
            VALUES ('I0', 'O0', 'P0', 1, 10, '2026-01-01', 0, 0)
        """)
        connection.commit()
        yield connection


def _run_application(
    connection: sqlite3.Connection,
    counts: dict[str, int],
    *,
    fail_before_ledger: bool = False,
) -> list[tuple[str, str, int, int, str]]:
    published = PublishedDelivery(
        "s3://retail-raw/raw/demo-v1/daily-001/manifest.json",
        "retail-raw",
        "raw/demo-v1/daily-001",
        BatchDelivery("demo-v1", "daily-001", "incremental", date(2026, 1, 2), ()),
        "a" * 64,
    )
    lock, *statements = application_statements(CORE, published, counts)
    assert lock == (
        "LOCK TABLE core_demo.applied_batches, core_demo.customers, "
        "core_demo.products, core_demo.orders, core_demo.order_items"
    )
    if fail_before_ledger:
        statements.insert(-2, "INSERT INTO missing_table VALUES (1)")
    with connection:
        connection.execute("BEGIN")
        for statement in statements[:-1]:
            connection.execute(statement)
        return connection.execute(statements[-1]).fetchall()


def _stage_rows(connection: sqlite3.Connection, customer_id: str = "C1") -> None:
    connection.execute("""
        INSERT INTO stage_customers
        VALUES ('C1', '2026-01-02', 'IE', 'retail', '2026-01-02', 1)
    """)
    connection.execute("""
        INSERT INTO stage_products
        VALUES ('P0', 'Notebook', 'Stationery', 8, 1, '2026-01-02', 1)
    """)
    connection.execute(
        "INSERT INTO stage_orders "
        "VALUES ('O1', ?, '2026-01-02', 'paid', '2026-01-02', 1)",
        (customer_id,),
    )
    connection.execute("""
        INSERT INTO stage_order_items
        VALUES ('I1', 'O1', 'P0', 2, 8, '2026-01-02', 1, 0)
    """)
    connection.commit()


def test_applies_same_delivery_references_and_preserves_historical_price(
    warehouse: sqlite3.Connection,
) -> None:
    _stage_rows(warehouse)

    result = _run_application(warehouse, dict.fromkeys(SNAPSHOT_LAYOUTS, 1))

    assert result[0][:2] == ("_batch", "applied")
    assert warehouse.execute("SELECT customer_id FROM core_demo.orders").fetchall() == [
        ("C1",)
    ]
    assert warehouse.execute(
        "SELECT current_price FROM core_demo.products"
    ).fetchall() == [(8,)]
    assert warehouse.execute(
        "SELECT item_id, unit_price FROM core_demo.order_items ORDER BY item_id"
    ).fetchall() == [("I0", 10), ("I1", 8)]
    assert warehouse.execute(
        "SELECT kind, manifest_sha256, customer_rows, product_rows, "
        "order_rows, order_item_rows "
        "FROM core_demo.applied_batches WHERE batch_id = 'daily-001'"
    ).fetchall() == [("incremental", "a" * 64, 1, 1, 1, 1)]


@pytest.mark.parametrize("failure", ["missing_reference", "warehouse_error"])
def test_application_failure_preserves_core_and_ledger(
    warehouse: sqlite3.Connection, failure: str
) -> None:
    if failure == "missing_reference":
        _stage_rows(warehouse, customer_id="C1 ")
        result = _run_application(warehouse, dict.fromkeys(SNAPSHOT_LAYOUTS, 1))
        assert result[0][:2] == ("_batch", "rejected")
        assert ("orders", "missing_customer_id", 1, 0, "O1") in result
    else:
        _stage_rows(warehouse)
        with pytest.raises(sqlite3.OperationalError, match="missing_table"):
            _run_application(
                warehouse, dict.fromkeys(SNAPSHOT_LAYOUTS, 1), fail_before_ledger=True
            )

    assert warehouse.execute("SELECT COUNT(*) FROM core_demo.customers").fetchone() == (
        0,
    )
    assert warehouse.execute(
        "SELECT current_price FROM core_demo.products"
    ).fetchone() == (10,)
    assert warehouse.execute(
        "SELECT COUNT(*) FROM core_demo.applied_batches"
    ).fetchone() == (1,)


@pytest.mark.parametrize(
    ("recorded_checksum", "outcome"),
    [("a" * 64, "already_applied"), ("b" * 64, "changed_batch")],
)
def test_recorded_delivery_blocks_core_writes(
    warehouse: sqlite3.Connection, recorded_checksum: str, outcome: str
) -> None:
    _stage_rows(warehouse)
    warehouse.execute(
        "INSERT INTO core_demo.applied_batches "
        "VALUES ('demo-v1', 'daily-001', 'incremental', ?, 1, 1, 1, 1)",
        (recorded_checksum,),
    )
    warehouse.commit()

    result = _run_application(warehouse, dict.fromkeys(SNAPSHOT_LAYOUTS, 1))

    assert result == [("_batch", outcome, 0, 0, "")]
    assert warehouse.execute("SELECT COUNT(*) FROM core_demo.customers").fetchone() == (
        0,
    )
    assert warehouse.execute(
        "SELECT current_price FROM core_demo.products"
    ).fetchone() == (10,)
    assert warehouse.execute(
        "SELECT COUNT(*) FROM core_demo.applied_batches"
    ).fetchone() == (2,)
