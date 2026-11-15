"""Hàm dùng chung cho mọi job: lấy SparkSession và thêm cột metadata khi nạp."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F


def get_spark(app_name: str = "instacart") -> SparkSession:
    """Trên Databricks trả về session có sẵn; ở local tạo session mới."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def add_ingest_metadata(df: DataFrame, source: str, ingest_ts: Column | None = None) -> DataFrame:
    """Thêm `_ingest_ts` và `_source` vào bản ghi khi nạp vào bronze."""
    return df.withColumn("_ingest_ts", ingest_ts if ingest_ts is not None else F.current_timestamp()).withColumn(
        "_source", F.lit(source)
    )
