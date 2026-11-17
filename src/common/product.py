"""Dựng danh mục sản phẩm bằng batch DataFrame, giữ nguyên grain theo product_id."""

from __future__ import annotations

import datetime as dt
from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.catalog_rows import canonical_catalog_rows, canonical_products

OFF_ATTRIBUTES = [
    "off_product_name",
    "brand",
    "categories_tags",
    "countries_tags",
    "nutriscore_grade",
    "nova_group",
    "energy_kcal_100g",
    "sugars_100g",
    "fat_100g",
    "image_url",
]
CATALOG_COUNTS = ["_products_n", "_aisles_n", "_departments_n"]
CATALOG_RESCUE_FLAGS = ["_product_id_unrescued", "_aisle_id_unrescued", "_department_id_unrescued"]
ENRICHMENT_COUNTS = ["_prices_n", "_matches_n", "_off_n"]


def as_of_date(value: str) -> dt.date:
    """Ngày cấu hình phải là ISO YYYY-MM-DD, không phụ thuộc ngày chạy hoặc timezone."""
    parsed = dt.date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError("as_of_date phải có dạng YYYY-MM-DD")
    return parsed


def _one(frame: DataFrame, keys: list[str], count_name: str, order=()) -> DataFrame:
    """Một dòng trước join, nhưng giữ số nguồn để expectation bắt xung đột thay vì che lỗi."""
    partition = Window.partitionBy(*keys)
    tie = F.sha2(F.to_json(F.struct(*[F.col(c) for c in sorted(frame.columns)])), 256).asc()
    return (
        frame.withColumn(count_name, F.count("*").over(partition))
        .withColumn("_rn", F.row_number().over(partition.orderBy(*order, tie)))
        .where("_rn = 1")
        .drop("_rn")
    )


def build_dim_product(
    products: DataFrame,
    aisles: DataFrame,
    departments: DataFrame,
    prices: DataFrame,
    promotions: DataFrame,
    matches: DataFrame,
    off: DataFrame,
    images: DataFrame,
    date: str,
) -> DataFrame:
    """Giá hiện hành; khuyến mãi tại ngày cấu hình; OFF/ảnh chỉ từ matched."""
    day = F.lit(as_of_date(date))
    p = canonical_products(products)
    a = canonical_catalog_rows(aisles, "aisle_id", ["aisle"], "_aisles_n")
    d = canonical_catalog_rows(departments, "department_id", ["department"], "_departments_n")
    # Không lấy MAX giá hoặc bản lịch sử cũ khi một sản phẩm không còn giá đang mở.
    current = prices.where(F.col("__END_AT").isNull()).select(
        "product_id", F.col("unit_price").alias("current_price"), "currency", "__START_AT"
    )
    pr = _one(current, ["product_id"], "_prices_n", [F.col("__START_AT").desc_nulls_last()]).drop("__START_AT")
    km = (
        promotions.where((F.col("start_date") <= day) & (F.col("end_date") >= day))
        .groupBy("product_id")
        .agg(F.max("discount_pct").alias("discount_pct"))
    )
    m = _one(
        matches.select("product_id", "off_code", "off_match_score", "off_match_status").distinct(),
        ["product_id"],
        "_matches_n",
    ).withColumn("off_code", F.when(F.col("off_match_status") == "matched", F.col("off_code")))
    # Một barcode có thể được tra cho nhiều sản phẩm: không bỏ product_id rồi chọn ngẫu nhiên một hit.
    detail = _one(
        off.select("product_id", "off_code", *OFF_ATTRIBUTES).distinct(), ["product_id", "off_code"], "_off_n"
    ).withColumnRenamed("categories_tags", "off_categories")
    # Ảnh có thể được bronze nạp lại: chọn file mới nhất, rồi path tăng dần khi hòa.
    image = (
        images.select("off_code", F.col("path").alias("image_path"), "modificationTime", "_ingest_ts")
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("off_code").orderBy(
                    F.col("image_path").isNotNull().desc(),
                    F.col("modificationTime").desc_nulls_last(),
                    F.col("_ingest_ts").desc_nulls_last(),
                    F.col("image_path").asc_nulls_last(),
                )
            ),
        )
        .where("_rn = 1")
        .select("off_code", "image_path")
    )
    joined = (
        p.join(a, "aisle_id", "left")
        .join(d, "department_id", "left")
        .join(pr, "product_id", "left")
        .join(km, "product_id", "left")
        .join(m, "product_id", "left")
        .join(detail, ["product_id", "off_code"], "left")
        .join(image, "off_code", "left")
        .withColumn("off_match_status", F.coalesce("off_match_status", F.lit("no_match")))
        .withColumn("off_match_score", F.coalesce("off_match_score", F.lit(0.0)))
        .withColumn("on_promotion", F.col("discount_pct").isNotNull())
    )
    unique = reduce(lambda a, b: a & b, [F.coalesce(F.col(c), F.lit(1)) == 1 for c in ENRICHMENT_COUNTS])
    catalog_unique = reduce(lambda a, b: a & b, [F.coalesce(F.col(c), F.lit(1)) == 1 for c in CATALOG_COUNTS])
    unrescued = reduce(lambda a, b: a & b, [F.coalesce(F.col(c), F.lit(True)) for c in CATALOG_RESCUE_FLAGS])
    return (
        joined.withColumn("_join_sources_unique", unique)
        .withColumn("_catalog_sources_unique", catalog_unique)
        .withColumn("_catalog_sources_unrescued", unrescued)
        .drop(*CATALOG_COUNTS, *CATALOG_RESCUE_FLAGS, *ENRICHMENT_COUNTS)
        .withColumn("dup_cnt", F.count("*").over(Window.partitionBy("product_id")))
    )
