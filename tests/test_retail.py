"""Kiểm DQ/chuẩn hóa và checkpoint dedup bằng Spark thật; không mô phỏng AUTO CDC."""

from __future__ import annotations

import datetime as dt
import json
import sys
import uuid
from decimal import Decimal
from functools import wraps
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, TimestampType

from common.retail import (
    PRICE_CHANGE_RULES,
    PRICE_COLUMNS,
    PROMO_CHANGE_RULES,
    PROMO_COLUMNS,
    assess_changes,
    deduplicate_changes,
    quarantine_changes,
)

PRICE_SCHEMA = (
    "product_id STRING, unit_price STRING, currency STRING, updated_at STRING, "
    "_op STRING, _lsn STRING, _seqval STRING, _rescued_data STRING, _source_file STRING"
)
PROMO_SCHEMA = (
    "promo_id STRING, product_id STRING, promo_type STRING, discount_pct STRING, start_date STRING, "
    "end_date STRING, updated_at STRING, _op STRING, _lsn STRING, _seqval STRING, "
    "_rescued_data STRING, _source_file STRING"
)


def price(**overrides):
    return {
        "product_id": "24852",
        "unit_price": "0.29",
        "currency": "USD",
        "updated_at": "2015-01-01 00:00:00",
        "_op": "0",
        "_lsn": "0x00000000000000000001",
        "_seqval": "0x00000000000000000000",
        "_rescued_data": None,
        "_source_file": "snapshot/part.parquet",
        **overrides,
    }


def promotion(**overrides):
    return {
        "promo_id": "P0001",
        "product_id": "24852",
        "promo_type": "percent_off",
        "discount_pct": "15",
        "start_date": "2026-03-01",
        "end_date": "2026-03-07",
        "updated_at": "2026-02-25 10:00:00",
        "_op": "0",
        "_lsn": "0x00000000000000000001",
        "_seqval": "0x00000000000000000000",
        "_rescued_data": None,
        "_source_file": "snapshot/part.parquet",
        **overrides,
    }


@pytest.fixture(autouse=True)
def utc_session(spark):
    settings = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.ansi.enabled": "true",
        "spark.sql.timestampType": "TIMESTAMP_NTZ",
    }
    previous = {key: spark.conf.get(key) for key in settings}
    for key, value in settings.items():
        spark.conf.set(key, value)
    try:
        yield
    finally:
        for key, value in previous.items():
            spark.conf.set(key, value)


def test_price_normalization_preserves_decimal_timestamp_and_log_order(spark):
    frame = assess_changes(spark.createDataFrame([price(_lsn="0X000000000000000000aF")], PRICE_SCHEMA), "prices")
    row = frame.first()
    assert row.unit_price == Decimal("0.29") and row._dq_failed == []
    # collect() trả datetime naive theo timezone của Python host; so epoch để kiểm đúng instant UTC.
    assert frame.select(F.unix_timestamp("updated_at")).first()[0] == int(
        dt.datetime(2015, 1, 1, tzinfo=dt.UTC).timestamp()
    )
    assert isinstance(frame.schema["updated_at"].dataType, TimestampType)
    assert isinstance(frame.schema["_lsn"].dataType, BinaryType)
    assert bytes(row._lsn) == bytes.fromhex("000000000000000000af")


def test_binary_lsn_is_accepted_and_wrong_length_is_quarantined(spark):
    raw = spark.createDataFrame([price()], PRICE_SCHEMA).withColumn("_lsn", F.unhex(F.substring("_lsn", 3, 20)))
    assert assess_changes(raw, "prices").first()._dq_failed == []
    invalid = raw.withColumn("_lsn", F.lit(b"\x01"))
    assert assess_changes(invalid, "prices").first()._dq_failed == ["has_lsn"]


@pytest.mark.parametrize(
    "overrides,failed",
    [
        ({"unit_price": "0"}, ["price_positive"]),
        ({"unit_price": "not-a-price"}, ["price_positive"]),
        ({"currency": "usd"}, ["currency_usd"]),
        ({"product_id": "bad"}, ["has_product_id"]),
        ({"updated_at": "bad"}, ["has_updated_at"]),
        ({"_lsn": "0x123"}, ["has_lsn"]),
        ({"_seqval": "0xGG000000000000000000"}, ["has_seqval"]),
        ({"_op": "bad"}, ["valid_operation"]),
        ({"_op": "5"}, ["valid_operation"]),
        ({"_rescued_data": '{"extra":1}'}, ["no_rescued"]),
    ],
)
def test_price_invalid_payload_is_retained_with_exact_failure_names(spark, overrides, failed):
    original = price(**overrides)
    assessed = assess_changes(spark.createDataFrame([original], PRICE_SCHEMA), "prices")
    quarantine = quarantine_changes(assessed).first().asDict()
    assert quarantine.pop("_dq_failed") == failed
    assert quarantine.pop("_quarantined_at") is not None
    assert quarantine == original


@pytest.mark.parametrize(
    "overrides,failed",
    [
        ({"discount_pct": "91"}, ["discount_range"]),
        ({"start_date": "2026-03-08"}, ["valid_window"]),
        ({"promo_id": None}, ["has_promo_id"]),
        ({"product_id": None}, ["has_product_id"]),
        ({"updated_at": "bad"}, ["has_updated_at"]),
        ({"promo_type": ""}, ["has_promo_type"]),
    ],
)
def test_promotion_invalid_payload_has_matching_quarantine(spark, overrides, failed):
    original = promotion(**overrides)
    result = quarantine_changes(assess_changes(spark.createDataFrame([original], PROMO_SCHEMA), "promotions")).first()
    assert result._dq_failed == failed
    assert result.promo_id == original["promo_id"] and result.discount_pct == original["discount_pct"]


def test_promo_delete_requires_no_payload_and_price_delete_is_quarantined(spark):
    promo = promotion(
        _op="1", product_id=None, promo_type=None, discount_pct=None, start_date=None, end_date=None, updated_at=None
    )
    assert assess_changes(spark.createDataFrame([promo], PROMO_SCHEMA), "promotions").first()._dq_failed == []
    tombstone = price(_op="1", unit_price=None, currency=None, updated_at="2026-04-01 00:00:00")
    checked = assess_changes(spark.createDataFrame([tombstone], PRICE_SCHEMA), "prices")
    assert quarantine_changes(checked).first()._dq_failed == ["no_price_delete"]


@pytest.mark.parametrize(
    "kind,schema,record", [("prices", PRICE_SCHEMA, price), ("promotions", PROMO_SCHEMA, promotion)]
)
def test_before_images_are_not_applied_or_misclassified_as_bad_rows(spark, kind, schema, record):
    raw = spark.createDataFrame([record(_op="3")], schema)
    assert assess_changes(raw, kind).count() == 0


def test_bad_duplicate_cannot_displace_good_event_before_dedup(spark):
    frame = spark.createDataFrame(
        [price(unit_price="-1"), price(), price(_source_file="replay/part.parquet")], PRICE_SCHEMA
    )
    checked = assess_changes(frame, "prices")
    valid = checked.where(F.size("_dq_failed") == 0)
    assert deduplicate_changes(valid, "prices").count() == 1
    assert quarantine_changes(checked).first().unit_price == "-1"


def test_conflicting_payload_same_sequence_is_not_silently_collapsed(spark):
    raw = spark.createDataFrame([price(), price(unit_price="0.39")], PRICE_SCHEMA)
    assert deduplicate_changes(assess_changes(raw, "prices"), "prices").count() == 2
    # Nguồn phải giải quyết xung đột này trước AUTO CDC; test không giả lập kết quả SCD2.


def test_price_snapshot_and_overlapping_after_image_are_one_upsert(spark):
    raw = spark.createDataFrame([price(_op="0"), price(_op="4", _lsn="0x00000000000000000002")], PRICE_SCHEMA)
    assert deduplicate_changes(assess_changes(raw, "prices"), "prices").count() == 1


def test_native_stream_dedup_preserves_late_business_event_and_ignores_replayed_file(spark, tmp_path):
    source = tmp_path / "incoming"
    source.mkdir()
    first = [price(), price(unit_price="0.27", updated_at="2026-03-15 00:00:00", _op="4")]
    (source / "01.json").write_text("\n".join(json.dumps(row) for row in first))
    raw = spark.readStream.schema(PRICE_SCHEMA).json(str(source))
    checked = assess_changes(raw, "prices").where(F.size("_dq_failed") == 0)
    name = "retail_dedup_" + uuid.uuid4().hex
    query = (
        deduplicate_changes(checked, "prices")
        .writeStream.format("memory")
        .queryName(name)
        .outputMode("append")
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .start()
    )
    try:
        query.processAllAvailable()
        assert spark.table(name).count() == 2
        replay_and_late = [
            {**first[1], "_source_file": "replayed/part.parquet"},
            price(unit_price="0.25", updated_at="2026-03-01 00:00:00", _op="4"),
        ]
        (source / "02.json").write_text("\n".join(json.dumps(row) for row in replay_and_late))
        query.processAllAvailable()
        rows = spark.table(name).orderBy("updated_at").collect()
        assert [row.unit_price for row in rows] == [Decimal("0.29"), Decimal("0.25"), Decimal("0.27")]
    finally:
        query.stop()
        spark.catalog.dropTempView(name)


@pytest.fixture
def pipeline(spark, monkeypatch):
    views, tables, targets, flows = {}, {}, {}, []

    def register(destination):
        def decorator(**kwargs):
            def decorate(function):
                destination[kwargs["name"]] = (function, kwargs)
                return function

            return decorate

        return decorator

    def expect_all_or_drop(rules):
        def decorate(function):
            @wraps(function)
            def filtered():
                frame = function()
                for condition in rules.values():
                    frame = frame.where(F.coalesce(F.expr(condition), F.lit(False)))
                return frame

            filtered.rules = rules
            return filtered

        return decorate

    def create_streaming_table(**kwargs):
        targets[kwargs["name"]] = kwargs

    def create_auto_cdc_flow(**kwargs):
        assert kwargs["target"] in targets
        flows.append(kwargs)

    monkeypatch.setitem(
        sys.modules,
        "pyspark.pipelines",
        SimpleNamespace(
            temporary_view=register(views),
            table=register(tables),
            expect_all_or_drop=expect_all_or_drop,
            create_streaming_table=create_streaming_table,
            create_auto_cdc_flow=create_auto_cdc_flow,
        ),
    )
    configuration = {
        "catalog": "test_catalog",
        "bronze": "p2_bronze",
        "silver": "p2_silver",
        "spark.sql.session.timeZone": "UTC",
    }
    previous = {key: spark.conf.get(key, None) for key in configuration}
    try:
        for key, value in configuration.items():
            spark.conf.set(key, value)
        module = ModuleType("silver_retail_under_test")
        source = Path(__file__).resolve().parents[1] / "src/pipelines/batch/silver_retail.py"
        exec(compile(source.read_text(), str(source), "exec"), module.__dict__)
        raw = {
            "`test_catalog`.`p2_bronze`.`product_prices_cdc`": spark.createDataFrame(
                [price(unit_price="-1"), price(), price(_source_file="replay/part.parquet")], PRICE_SCHEMA
            ),
            "`test_catalog`.`p2_bronze`.`promotions_cdc`": spark.createDataFrame(
                [
                    promotion(_op="4", updated_at="2026-03-01 00:00:00"),
                    promotion(_op="1", updated_at="2026-02-01 00:00:00", _lsn="0x00000000000000000002"),
                    promotion(promo_id="bad", discount_pct="101"),
                ],
                PROMO_SCHEMA,
            ),
        }

        def read_table(name):
            return raw[name] if name in raw else views[name][0]()

        module.spark = SimpleNamespace(readStream=SimpleNamespace(table=read_table))
        yield SimpleNamespace(views=views, tables=tables, targets=targets, flows=flows)
    finally:
        for key, value in previous.items():
            spark.conf.unset(key) if value is None else spark.conf.set(key, value)


def test_price_flow_contract_and_dq_runs_before_duplicate_removal(pipeline):
    flow = next(flow for flow in pipeline.flows if flow["name"] == "product_prices_scd2")
    assert flow["keys"] == ["product_id"] and flow["stored_as_scd_type"] == 2
    assert flow["track_history_column_list"] == ["unit_price", "currency"]
    assert pipeline.views["product_prices_valid"][0].rules is PRICE_CHANGE_RULES
    changes = pipeline.views[flow["source"]][0]()
    assert changes.columns == PRICE_COLUMNS and changes.count() == 1
    assert isinstance(changes.select(flow["sequence_by"].alias("sequence")).schema["sequence"].dataType, TimestampType)
    assert flow["except_column_list"] == ["_op", "_lsn", "_seqval"]
    assert "apply_as_deletes" not in flow
    target = pipeline.targets[flow["target"]]
    assert target["table_properties"]["delta.enableChangeDataFeed"] == "true"


def test_promo_delete_is_sequenced_after_update_despite_older_updated_at(pipeline):
    flow = next(flow for flow in pipeline.flows if flow["name"] == "promotions_scd1")
    assert flow["keys"] == ["promo_id"] and flow["stored_as_scd_type"] == 1
    assert pipeline.views["promotions_valid"][0].rules is PROMO_CHANGE_RULES
    changes = pipeline.views[flow["source"]][0]()
    assert changes.columns == PROMO_COLUMNS
    sequenced = changes.select("*", flow["sequence_by"].alias("sequence"), flow["apply_as_deletes"].alias("deleted"))
    assert all(isinstance(field.dataType, BinaryType) for field in sequenced.schema["sequence"].dataType.fields)
    assert [(row._op, row.deleted) for row in sequenced.orderBy("sequence").collect()] == [(4, False), (1, True)]


def test_both_quarantines_keep_business_errors_and_target_schema_is_personal(pipeline):
    assert set(pipeline.tables) == {
        "`test_catalog`.`p2_silver`.`quarantine_product_prices`",
        "`test_catalog`.`p2_silver`.`quarantine_promotions`",
    }
    assert pipeline.tables["`test_catalog`.`p2_silver`.`quarantine_product_prices`"][0]().first().unit_price == "-1"
    assert pipeline.tables["`test_catalog`.`p2_silver`.`quarantine_promotions`"][0]().first().discount_pct == "101"
    assert all("`p2_silver`" in target for target in pipeline.targets)
