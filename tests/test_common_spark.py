from __future__ import annotations

from pyspark.sql import functions as F

from common.spark import add_ingest_metadata


def test_add_ingest_metadata_adds_columns(spark):
    df = spark.createDataFrame([(1, "Banana")], ["product_id", "product_name"])

    out = add_ingest_metadata(df, source="zenodo", ingest_ts=F.lit("2026-11-15 00:00:00").cast("timestamp"))

    assert out.columns == ["product_id", "product_name", "_ingest_ts", "_source"]
    row = out.first()
    assert row["_source"] == "zenodo"
    assert str(row["_ingest_ts"]) == "2026-11-15 00:00:00"
