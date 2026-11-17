from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from common.sim_time import SIM_END_ANCHOR, order_time_sim_sql, order_ts_sim

FIXTURE = Path(__file__).parent / "fixtures" / "sim_time_orders.csv"


@pytest.mark.parametrize(
    ("user_id", "hour", "days_back", "expected"),
    [
        (1, 8, 0, "2026-03-30T08:00:00+00:00"),
        (30, 0, 0, "2026-03-01T00:00:00+00:00"),
        (31, 23, 0, "2026-03-30T23:00:00+00:00"),
        (2, 15, 58, "2026-01-30T15:00:00+00:00"),
        (300001, 8, 35, "2026-02-23T08:00:00+00:00"),
    ],
)
def test_python_timestamp_has_fixed_anchor_and_utc(user_id, hour, days_back, expected):
    assert SIM_END_ANCHOR == "2026-03-01"
    result = order_ts_sim(user_id, hour, days_back)
    assert result == datetime.fromisoformat(expected)
    assert result.tzinfo is UTC


@pytest.mark.parametrize(
    ("args", "error"),
    [
        ((0, 8), ValueError),
        ((-1, 8), ValueError),
        ((1, -1), ValueError),
        ((1, 24), ValueError),
        ((1, 8, -1), ValueError),
        ((1.5, 8), TypeError),
        ((1, 8.5), TypeError),
        ((1, 8, 0.5), TypeError),
        ((True, 8), TypeError),
        ((1, False), TypeError),
        ((1, 8, None), TypeError),
    ],
)
def test_python_rejects_invalid_inputs(args, error):
    with pytest.raises(error):
        order_ts_sim(*args)


@pytest.mark.parametrize("timezone", ["UTC", "Asia/Ho_Chi_Minh", "America/New_York"])
@pytest.mark.parametrize("ansi", ["true", "false"])
def test_sql_and_python_agree_on_complete_histories(spark, timezone, ansi):
    # Đọc CSV thành STRING như bronze; fixture cố ý đảo thứ tự đơn và có NULL.
    with FIXTURE.open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    orders = spark.read.option("header", True).csv(str(FIXTURE))
    orders = orders.withColumn("_ingest_ts", F.to_timestamp("_ingest_ts"))
    # Bản cũ có khoảng cách/giờ sai phải bị loại TRƯỚC khi tính tổng ngày.
    stale = (
        orders.where("order_id = '102'")
        .withColumn("days_since_prior_order", F.lit("1.0"))
        .withColumn("order_hour_of_day", F.lit("3"))
        .withColumn("_ingest_ts", F.lit("2026-11-16 01:00:00").cast("timestamp"))
    )
    orders.unionByName(stale).createOrReplaceTempView("sim_time_orders")
    original_timezone = spark.conf.get("spark.sql.session.timeZone")
    original_type = spark.conf.get("spark.sql.timestampType")
    original_ansi = spark.conf.get("spark.sql.ansi.enabled")
    try:
        spark.conf.set("spark.sql.session.timeZone", timezone)
        spark.conf.set("spark.sql.ansi.enabled", ansi)
        # SQL phải trả cùng một instant ngay cả khi default timestampType là NTZ.
        spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")
        result = spark.sql(order_time_sim_sql("sim_time_orders"))
        actual = {
            row.order_id: row.epoch
            for row in result.selectExpr("order_id", "unix_seconds(order_ts_sim) AS epoch").collect()
        }
    finally:
        spark.conf.set("spark.sql.session.timeZone", original_timezone)
        spark.conf.set("spark.sql.timestampType", original_type)
        spark.conf.set("spark.sql.ansi.enabled", original_ansi)
        spark.catalog.dropTempView("sim_time_orders")

    assert len(actual) == len(rows)
    for user in {row["user_id"] for row in rows}:
        history = sorted((row for row in rows if row["user_id"] == user), key=lambda row: int(row["order_number"]))
        days_back = 0
        for row in reversed(history):
            expected = datetime.fromisoformat(row["expected_utc"])
            python_time = order_ts_sim(int(user), int(row["order_hour_of_day"]), days_back)
            assert python_time == expected
            assert actual[int(row["order_id"])] == int(expected.timestamp())
            days_back += int(float(row["days_since_prior_order"] or 0))


def test_sql_orders_numerically_beyond_nine(spark):
    rows = [
        (str(n), "30", "test" if n == 11 else "prior", str(n), "8", None if n == 1 else "1.0", "2026-11-17")
        for n in range(1, 12)
    ]
    spark.createDataFrame(
        rows,
        "order_id string, user_id string, eval_set string, order_number string, order_hour_of_day string, "
        "days_since_prior_order string, _ingest_ts string",
    ).createOrReplaceTempView("long_history")
    try:
        actual = (
            spark.sql(order_time_sim_sql("long_history"))
            .selectExpr("order_id", "unix_seconds(order_ts_sim) AS epoch")
            .collect()
        )
        assert len(actual) == 11
        for row in actual:
            assert row.epoch == int(order_ts_sim(30, 8, 11 - row.order_id).timestamp())
    finally:
        spark.catalog.dropTempView("long_history")


@pytest.mark.parametrize("table", ["", "orders; DROP TABLE orders", "catalog..orders", "a.b.c.d", "orders WHERE 1=1"])
def test_sql_source_requires_table_identifier(table):
    with pytest.raises(ValueError, match="orders_table"):
        order_time_sim_sql(table)
