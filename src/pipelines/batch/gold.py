"""Materialized view Gold cho đơn hàng đã hợp nhất và giá tại thời điểm đặt."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from common.gold import build_fact_order_lines

spark = SparkSession.active()
CATALOG = spark.conf.get("catalog")
SILVER = spark.conf.get("silver")
GOLD = spark.conf.get("gold")


def table_name(schema: str, name: str) -> str:
    return ".".join("`" + part.replace("`", "``") + "`" for part in (CATALOG, schema, name))


if spark.conf.get("enable_gold_fact", "false").lower() == "true":

    @dp.materialized_view(
        name=table_name(GOLD, "fact_order_lines"),
        cluster_by=["order_date", "department_id"],
        table_properties={"quality": "gold"},
        comment="Một dòng mỗi món trong đơn, giá tại thời điểm đặt và doanh thu giả lập.",
    )
    @dp.expect_or_fail("has_order", "user_id IS NOT NULL AND order_ts_sim IS NOT NULL")
    @dp.expect_or_fail("has_product", "department_id IS NOT NULL AND aisle_id IS NOT NULL")
    @dp.expect("priced", "unit_price IS NOT NULL")
    def fact_order_lines():
        return build_fact_order_lines(
            spark.read.table(table_name(SILVER, "order_items")),
            spark.read.table(table_name(SILVER, "orders")),
            spark.read.table(table_name(SILVER, "dim_product")),
            spark.read.table(table_name(SILVER, "product_prices_history")),
            spark.read.table(table_name(SILVER, "promotions")),
        )
