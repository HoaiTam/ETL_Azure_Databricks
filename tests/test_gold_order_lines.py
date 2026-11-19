"""Kiểm grain, biên thời gian và giá trị tiền của fact Gold bằng Spark local."""

import runpy
import sys
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from common.gold import build_fact_order_lines

GOLD_PIPELINE = Path(__file__).resolve().parents[1] / "src/pipelines/batch/gold.py"


@pytest.fixture
def utc_spark(spark):
    key = "spark.sql.session.timeZone"
    previous = spark.conf.get(key)
    spark.conf.set(key, "UTC")
    try:
        yield spark
    finally:
        spark.conf.set(key, previous)


def test_fact_keeps_keys_selects_historical_price_and_resolves_overlapping_promotions(utc_spark):
    spark = utc_spark
    seam = datetime(2026, 3, 8, tzinfo=UTC)
    day_after = datetime(2026, 3, 9, tzinfo=UTC)
    orders = spark.createDataFrame(
        [
            (1, "batch", 100, 3, 16, datetime(2026, 3, 7, 23, 59, 59, tzinfo=UTC)),
            (2, "stream", 101, 4, 17, seam),
            (3, "stream", 102, 5, 18, day_after),
            (4, "batch", 103, 6, 19, day_after),
            (5, "batch", 104, 0, 20, day_after),
        ],
        "order_id LONG, source STRING, user_id LONG, order_dow INT, order_hour_of_day INT, order_ts_sim TIMESTAMP",
    )
    items = spark.createDataFrame(
        [
            (1, 10, 1, 3, "batch"),
            (2, 10, 0, 2, "stream"),
            (3, 10, 1, 1, "stream"),
            (3, 11, 0, 2, "stream"),
            (4, 10, 1, 1, "stream"),
            (5, 12, 0, 1, "batch"),
        ],
        "order_id LONG, product_id LONG, reordered INT, add_to_cart_order INT, source STRING",
    )
    products = spark.createDataFrame(
        [(10, 2, 7, Decimal("99.00")), (11, 2, 8, Decimal("11.00"))],
        "product_id LONG, department_id INT, aisle_id INT, current_price DECIMAL(10,2)",
    )
    prices = spark.createDataFrame(
        [
            (10, Decimal("10.00"), datetime(2015, 1, 1, tzinfo=UTC), seam),
            (10, Decimal("20.00"), seam, None),
            (12, Decimal("8.00"), seam, None),
        ],
        "product_id LONG, unit_price DECIMAL(10,2), __START_AT TIMESTAMP, __END_AT TIMESTAMP",
    )
    promotions = spark.createDataFrame(
        [
            (10, date(2026, 3, 7), date(2026, 3, 8), Decimal("10.00")),
            (10, date(2026, 3, 8), date(2026, 3, 8), Decimal("25.50")),
            (10, date(2026, 4, 1), date(2026, 4, 5), Decimal("90.00")),
        ],
        "product_id LONG, start_date DATE, end_date DATE, discount_pct DECIMAL(5,2)",
    )

    fact = build_fact_order_lines(items, orders, products, prices, promotions).collect()
    by_key = {(row.order_id, row.product_id): row for row in fact}
    assert len(fact) == len(by_key) == 6
    assert set(by_key) == {(1, 10), (2, 10), (3, 10), (3, 11), (4, 10), (5, 12)}

    old, boundary, regular = (by_key[(order, 10)] for order in (1, 2, 3))
    assert (old.unit_price, old.discount_pct, old.revenue_sim) == (Decimal("10.00"), Decimal("10.00"), Decimal("9.00"))
    assert (boundary.unit_price, boundary.discount_pct, boundary.revenue_sim) == (
        Decimal("20.00"),
        Decimal("25.50"),
        Decimal("14.90"),
    )
    assert (regular.unit_price, regular.discount_pct, regular.revenue_sim) == (Decimal("20.00"), None, Decimal("20.00"))
    assert (boundary.source, boundary.order_dow, boundary.order_hour_of_day, boundary.order_date) == (
        "stream",
        4,
        17,
        date(2026, 3, 8),
    )
    assert (boundary.department_id, boundary.aisle_id, boundary.reordered, boundary.add_to_cart_order) == (2, 7, 0, 2)
    assert by_key[(3, 11)].unit_price is None and by_key[(3, 11)].revenue_sim is None
    assert by_key[(4, 10)].user_id is None and by_key[(4, 10)].order_date is None
    assert by_key[(5, 12)].department_id is None


@pytest.mark.parametrize("enabled", [None, "true"])
def test_gold_pipeline_is_opt_in_and_registers_expected_clustering(spark, monkeypatch, enabled):
    definitions = []
    expectations = []

    def materialized_view(**kwargs):
        definitions.append(kwargs)
        return lambda function: function

    def expect(name, condition):
        expectations.append((name, condition))
        return lambda function: function

    dp = SimpleNamespace(materialized_view=materialized_view, expect=expect, expect_or_fail=expect)
    monkeypatch.setitem(sys.modules, "pyspark.pipelines", dp)
    config = {"catalog": "test_catalog", "silver": "p2_silver", "gold": "p2_gold", "enable_gold_fact": enabled}
    previous = {key: spark.conf.get(key, None) for key in config}
    try:
        for key, value in config.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)
        runpy.run_path(str(GOLD_PIPELINE))
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)

    if enabled:
        (definition,) = definitions
        assert definition["name"] == "`test_catalog`.`p2_gold`.`fact_order_lines`"
        assert definition["cluster_by"] == ["order_date", "department_id"]
        assert ("priced", "unit_price IS NOT NULL") in expectations
    else:
        assert definitions == expectations == []
