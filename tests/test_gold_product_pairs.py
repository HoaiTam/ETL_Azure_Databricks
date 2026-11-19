"""Kiểm baseline cặp trên giỏ prior bằng SQL Spark thực tế."""

import runpy
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from pyspark.sql import SparkSession

PIPELINE = Path(__file__).resolve().parents[1] / "src/pipelines/batch/gold.py"


def test_pair_baseline_filters_split_deduplicates_items_and_enforces_support(spark, monkeypatch):
    ids = ((i, "prior", [2, 1, 2]) for i in range(52))
    weak = ((100 + i, "prior", [3, 1]) for i in range(49))
    held_out = [(200, "holdout", [1, 3]), (201, "holdout", [1, 3])]
    spark.createDataFrame(
        [*ids, *weak, *held_out], "order_id INT, split STRING, items ARRAY<INT>"
    ).createOrReplaceTempView("pair_test_baskets")
    spark.createDataFrame([(1, 200), (2, 100), (3, 80)], "product_id INT, times_ordered LONG").createOrReplaceTempView(
        "pair_test_products"
    )
    config = {"catalog": "instacart", "silver": "test_silver", "gold": "test_gold", "enable_gold_fact": "true"}

    def sql(query):
        return spark.sql(
            query.replace("`instacart`.`test_silver`.`baskets`", "pair_test_baskets").replace(
                "`instacart`.`test_gold`.`product_360`", "pair_test_products"
            )
        )

    def decorator(*args, **kwargs):
        return lambda function: function

    monkeypatch.setitem(
        sys.modules,
        "pyspark.pipelines",
        SimpleNamespace(
            materialized_view=decorator, expect_or_fail=decorator, expect=decorator, expect_or_drop=decorator
        ),
    )
    monkeypatch.setattr(SparkSession, "active", lambda: SimpleNamespace(conf=config, sql=sql))
    try:
        assert "product_pair_counts" not in runpy.run_path(str(PIPELINE))
        config["enable_baskets"] = "true"
        result = runpy.run_path(str(PIPELINE))["product_pair_counts"]().collect()
        (pair,) = result
        assert (pair.p1, pair.p2, pair.pair_count) == (1, 2, 52)
        assert pair.support == pytest.approx(52 / 101)
        assert pair.confidence_p1_to_p2 == pytest.approx(52 / 200)
        assert pair.lift == pytest.approx(52 * 101 / (200 * 100))
    finally:
        spark.catalog.dropTempView("pair_test_baskets")
        spark.catalog.dropTempView("pair_test_products")
