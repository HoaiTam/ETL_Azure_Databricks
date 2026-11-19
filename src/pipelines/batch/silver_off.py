# Databricks notebook source
# MAGIC %pip install rapidfuzz==3.14.6

# COMMAND ----------

"""OFF và bảng cầu nối tên; dim_product được triển khai riêng ở AD-DE-14."""

from pyspark import pipelines as dp
from pyspark.cloudpickle import register_pickle_by_value
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import off, text
from common.off import (
    MATCH_RULES,
    assess_off,
    clean_off,
    product_matches,
    quarantine_off,
    score_series,
)

spark = SparkSession.active()
CATALOG = spark.conf.get("catalog")
BRONZE = spark.conf.get("bronze")
SILVER = spark.conf.get("silver")


def table_name(schema: str, name: str) -> str:
    return ".".join("`" + part.replace("`", "``") + "`" for part in (CATALOG, schema, name))


if spark.conf.get("enable_off", "false").lower() == "true":
    # Worker có RapidFuzz từ môi trường pipeline, nhưng có thể không import được src/common.
    register_pickle_by_value(off)
    register_pickle_by_value(text)
    match_score = F.pandas_udf(score_series, "double")

    @dp.materialized_view(
        name=table_name(SILVER, "off_products_clean"),
        comment="OFF đã làm phẳng; bản hợp lệ mới nhất mỗi cặp product_id/off_code.",
        cluster_by=["product_id"],
    )
    @dp.expect("has_name", "off_product_name IS NOT NULL AND length(trim(off_product_name)) > 0")
    def off_products_clean():
        return clean_off(assess_off(spark.read.table(table_name(BRONZE, "off_products"))))

    @dp.materialized_view(
        name=table_name(SILVER, "quarantine_off_products"),
        comment="Mọi bản OFF sai khóa/ingest/rescue, giữ raw và các tên luật lỗi trước khử trùng.",
    )
    def quarantine_off_products():
        return quarantine_off(assess_off(spark.read.table(table_name(BRONZE, "off_products"))))

    @dp.materialized_view(
        name=table_name(SILVER, "product_off_match"),
        comment="Một ứng viên tốt nhất mỗi sản phẩm; chưa crawl/không có ứng viên Mỹ nhận no_match.",
        cluster_by=["product_id"],
    )
    @dp.expect_all_or_fail(MATCH_RULES)
    def product_off_match():
        return product_matches(
            spark.read.table(table_name(BRONZE, "products")),
            spark.read.table(table_name(SILVER, "off_products_clean")),
            match_score,
        )
