"""Fact món trong đơn với giá SCD2 và mức giảm cao nhất đang có hiệu lực."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_fact_order_lines(
    items: DataFrame, orders: DataFrame, products: DataFrame, prices: DataFrame, promotions: DataFrame
) -> DataFrame:
    headers = orders.select(
        "order_id",
        "source",
        "user_id",
        "order_dow",
        "order_hour_of_day",
        "order_ts_sim",
        F.to_date("order_ts_sim").alias("order_date"),
    )

    # Chỉ gộp ở các ngày có header. Cửa sổ KM có thể chồng nhau hoặc rất dài.
    order_days = F.broadcast(headers.select("order_date").distinct())
    daily_discounts = (
        promotions.join(order_days, F.col("order_date").between(F.col("start_date"), F.col("end_date")))
        .groupBy("product_id", "order_date")
        .agg(F.max("discount_pct").alias("discount_pct"))
    )

    # Giữ mọi khóa món khi thiếu header, danh mục hoặc giá để DQ nhìn thấy lỗi nguồn.
    lines = (
        items.select("order_id", "product_id", "reordered", "add_to_cart_order", "source")
        .join(headers, ["order_id", "source"], "left")
        .join(products.select("product_id", "department_id", "aisle_id"), "product_id", "left")
        .join(daily_discounts, ["product_id", "order_date"], "left")
        .alias("line")
    )
    history = prices.select("product_id", "unit_price", "__START_AT", "__END_AT").alias("price")
    priced = lines.join(
        history,
        (F.col("line.product_id") == F.col("price.product_id"))
        & (F.col("line.order_ts_sim") >= F.col("price.__START_AT"))
        & (F.col("price.__END_AT").isNull() | (F.col("line.order_ts_sim") < F.col("price.__END_AT"))),
        "left",
    )
    revenue = F.round(F.col("unit_price") * (1 - F.coalesce(F.col("discount_pct"), F.lit(0)) / 100), 2)
    return priced.select(
        "order_id",
        "user_id",
        "line.product_id",
        "reordered",
        "add_to_cart_order",
        "source",
        "order_dow",
        "order_hour_of_day",
        "order_ts_sim",
        "order_date",
        "department_id",
        "aisle_id",
        "unit_price",
        "discount_pct",
        revenue.cast("decimal(10,2)").alias("revenue_sim"),
    )
