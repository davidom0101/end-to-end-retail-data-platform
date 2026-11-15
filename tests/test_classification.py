import sqlite3
from collections.abc import Iterator
from contextlib import closing

import pytest

from retail_pipeline.classification import (
    classification_sql,
    classification_summary_sql,
)


@pytest.fixture
def warehouse() -> Iterator[sqlite3.Connection]:
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.create_function(
            "OCTET_LENGTH",
            1,
            lambda value: len(value.encode()) if value is not None else None,
        )
        connection.execute("ATTACH ':memory:' AS core_demo")
        columns = (
            "product_id, name, category, current_price, is_active, "
            "updated_at, source_sequence"
        )
        connection.execute(f"CREATE TABLE stage_products ({columns}, event_id)")
        connection.execute(f"CREATE TABLE core_demo.products ({columns})")
        yield connection


@pytest.mark.parametrize("reverse_order", [False, True])
def test_newest_timestamp_then_sequence_wins_and_replays_collapse(
    warehouse: sqlite3.Connection, reverse_order: bool
) -> None:
    warehouse.executemany(
        "INSERT INTO core_demo.products VALUES (?, ?, 'Tools', 89.99, 1, ?, ?)",
        [
            ("P001", "Drill", "2026-01-02 09:00:00", 1),
            ("P003", "Saw", "2026-01-02 09:00:00", 1),
        ],
    )
    records = [
        ("P001", "Drill", "2026-01-02 09:00:00", 1, "replay-1"),
        ("P001", "Drill", "2026-01-02 09:00:00", 1, "replay-2"),
        ("P001", "Old drill", "2026-01-01 09:00:00", 99, "old"),
        ("P001", "Drill with case", "2026-01-02 09:00:00", 2, "update"),
        ("P002", "Old model", "2026-01-01 09:00:00", 99, "older-new-key"),
        ("P002", "New model", "2026-01-02 09:00:00", 1, "newer-new-key"),
        ("P003", "Old saw", "2026-01-01 09:00:00", 99, "stale-only"),
    ]
    warehouse.executemany(
        "INSERT INTO stage_products VALUES (?, ?, 'Tools', 89.99, 1, ?, ?, ?)",
        reversed(records) if reverse_order else records,
    )

    warehouse.execute(classification_sql("products", "core_demo"))

    candidates = warehouse.execute(
        "SELECT product_id, name, decision FROM classified_products "
        "WHERE decision IN ('insert', 'update') ORDER BY product_id"
    ).fetchall()
    assert candidates == [
        ("P001", "Drill with case", "update"),
        ("P002", "New model", "insert"),
    ]
    counts = warehouse.execute(classification_summary_sql({"products": 7})).fetchall()
    assert {
        decision: (versions, repeated) for _, decision, versions, repeated, _ in counts
    } == {
        "duplicate": (1, 1),
        "update": (1, 0),
        "insert": (1, 0),
        "stale": (3, 0),
    }


def test_checks_conflicting_old_versions_even_when_newer_version_exists(
    warehouse: sqlite3.Connection,
) -> None:
    warehouse.execute(
        "INSERT INTO core_demo.products VALUES ('P001', 'Drill', 'Tools', 89.99, 1, "
        "'2026-01-02 09:00:00', 1)"
    )
    warehouse.executemany(
        "INSERT INTO stage_products VALUES ('P001', ?, 'Tools', 89.99, 1, ?, 1, ?)",
        [
            ("Original", "2026-01-01 09:00:00", "old-1"),
            ("Different original", "2026-01-01 09:00:00", "old-2"),
            ("Different drill", "2026-01-02 09:00:00", "equal-core"),
            ("Drill with case", "2026-01-03 09:00:00", "new"),
        ],
    )

    warehouse.execute(classification_sql("products", "core_demo"))

    counts = warehouse.execute(classification_summary_sql({"products": 4})).fetchall()
    assert {decision: (versions, key) for _, decision, versions, _, key in counts} == {
        "conflict_in_delivery": (2, "P001"),
        "conflict_with_core": (1, "P001"),
        "update": (1, "P001"),
    }


def test_v1_order_item_matches_null_and_zero_discounts(
    warehouse: sqlite3.Connection,
) -> None:
    columns = (
        "item_id, order_id, product_id, quantity, unit_price, "
        "updated_at, source_sequence"
    )
    warehouse.execute(f"CREATE TABLE stage_order_items ({columns}, discount_amount)")
    warehouse.execute(
        f"CREATE TABLE core_demo.order_items ({columns}, discount_amount)"
    )
    warehouse.executemany(
        "INSERT INTO core_demo.order_items "
        "VALUES (?, 'O1', 'P1', 1, 10, '2026-01-01', 1, ?)",
        [("I1", None), ("I2", 0), ("I3", 2)],
    )
    warehouse.execute(
        "INSERT INTO stage_order_items SELECT item_id, order_id, product_id, quantity, "
        "unit_price, updated_at, source_sequence, 0 FROM core_demo.order_items"
    )

    warehouse.execute(classification_sql("order_items", "core_demo"))

    assert warehouse.execute(
        "SELECT item_id, decision, discount_amount FROM classified_order_items "
        "ORDER BY item_id"
    ).fetchall() == [
        ("I1", "duplicate", 0),
        ("I2", "duplicate", 0),
        ("I3", "conflict_with_core", 0),
    ]


def test_v2_discount_participates_in_version_comparison(
    warehouse: sqlite3.Connection,
) -> None:
    columns = (
        "item_id, order_id, product_id, quantity, unit_price, "
        "updated_at, source_sequence, discount_amount"
    )
    warehouse.execute(f"CREATE TABLE stage_order_items ({columns})")
    warehouse.execute(f"CREATE TABLE core_demo.order_items ({columns})")
    warehouse.execute(
        "INSERT INTO core_demo.order_items "
        "VALUES ('I1', 'O1', 'P1', 1, 10, '2026-01-01', 1, 2)"
    )
    warehouse.execute(
        "INSERT INTO stage_order_items "
        "VALUES ('I1', 'O1', 'P1', 1, 10, '2026-01-01', 1, 3)"
    )
    warehouse.execute(
        "INSERT INTO stage_order_items "
        "VALUES ('I1', 'O1', 'P1', 1, 10, '2026-01-02', 1, 4)"
    )
    warehouse.execute(classification_sql("order_items", "core_demo"))
    assert warehouse.execute(
        "SELECT decision, discount_amount FROM classified_order_items "
        "ORDER BY discount_amount"
    ).fetchall() == [("conflict_with_core", 3), ("update", 4)]
