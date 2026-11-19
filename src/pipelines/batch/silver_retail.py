"""Silver retail: lịch sử giá SCD2 theo business time, khuyến mãi SCD1 theo log SQL Server."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.retail import (
    PRICE_CHANGE_RULES,
    PROMO_CHANGE_RULES,
    assess_changes,
    deduplicate_changes,
    quarantine_changes,
)

spark = SparkSession.active()
CATALOG = spark.conf.get("catalog")
BRONZE = spark.conf.get("bronze")
SILVER = spark.conf.get("silver")


def table_name(schema: str, table: str) -> str:
    return ".".join("`" + part.replace("`", "``") + "`" for part in (CATALOG, schema, table))


@dp.temporary_view(name="product_prices_valid", comment="DQ giá trước khi loại bản sao.")
@dp.expect_all_or_drop(PRICE_CHANGE_RULES)
def product_prices_valid():
    return assess_changes(spark.readStream.table(table_name(BRONZE, "product_prices_cdc")), "prices")


@dp.temporary_view(name="product_prices_changes", comment="Sự kiện giá hợp lệ, nguồn AUTO CDC SCD2.")
def product_prices_changes():
    return deduplicate_changes(spark.readStream.table("product_prices_valid"), "prices")


@dp.temporary_view(name="promotions_valid", comment="DQ khuyến mãi, không chặn tombstone thiếu business payload.")
@dp.expect_all_or_drop(PROMO_CHANGE_RULES)
def promotions_valid():
    return assess_changes(spark.readStream.table(table_name(BRONZE, "promotions_cdc")), "promotions")


@dp.temporary_view(name="promotions_changes", comment="Sự kiện khuyến mãi hợp lệ, giữ thứ tự LSN/seqval.")
def promotions_changes():
    return deduplicate_changes(spark.readStream.table("promotions_valid"), "promotions")


@dp.table(name=table_name(SILVER, "quarantine_product_prices"), table_properties={"quality": "silver"})
def quarantine_product_prices():
    return quarantine_changes(
        assess_changes(spark.readStream.table(table_name(BRONZE, "product_prices_cdc")), "prices")
    )


@dp.table(name=table_name(SILVER, "quarantine_promotions"), table_properties={"quality": "silver"})
def quarantine_promotions():
    return quarantine_changes(
        assess_changes(spark.readStream.table(table_name(BRONZE, "promotions_cdc")), "promotions")
    )


dp.create_streaming_table(
    name=table_name(SILVER, "product_prices_history"),
    comment="SCD2 giá, khoảng hiệu lực [__START_AT, __END_AT) theo updated_at UTC.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
)
dp.create_auto_cdc_flow(
    name="product_prices_scd2",
    target=table_name(SILVER, "product_prices_history"),
    source="product_prices_changes",
    keys=["product_id"],
    sequence_by=F.col("updated_at"),
    except_column_list=["_op", "_lsn", "_seqval"],
    stored_as_scd_type=2,
    track_history_column_list=["unit_price", "currency"],
)

dp.create_streaming_table(
    name=table_name(SILVER, "promotions"),
    comment="SCD1 khuyến mãi hiện hành; delete xếp theo LSN/seqval, không theo updated_at.",
    table_properties={"quality": "silver"},
)
dp.create_auto_cdc_flow(
    name="promotions_scd1",
    target=table_name(SILVER, "promotions"),
    source="promotions_changes",
    keys=["promo_id"],
    sequence_by=F.struct("_lsn", "_seqval"),
    apply_as_deletes=F.expr("_op = 1"),
    except_column_list=["_op", "_lsn", "_seqval"],
    stored_as_scd_type=1,
)
