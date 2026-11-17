"""Chuẩn hóa/DQ header và món hàng Instacart trước các dataset silver."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from common.dq import ITEM_RULES, ORDER_RULES, dq_failed

HEADER_COLUMNS = [
    "order_id",
    "user_id",
    "eval_set",
    "order_number",
    "order_dow",
    "order_hour_of_day",
    "days_since_prior_order",
    "_ingest_ts",
]
ITEM_COLUMNS = [
    "order_id",
    "product_id",
    "add_to_cart_order",
    "reordered",
    "_ingest_ts",
    "_rescued_data",
    "_source_file",
    "_file_mtime",
]

ORDER_INPUT_RULES = {
    **ORDER_RULES,
    "has_header_keys": "coalesce(order_id > 0 AND user_id > 0, false)",
    "valid_split": "coalesce(eval_set IN ('prior', 'train', 'test'), false)",
    "valid_order_number": "coalesce(order_number >= 1, false)",
    "parsed_gap": "_raw.days_since_prior_order IS NULL OR "
    "trim(cast(_raw.days_since_prior_order AS STRING)) = '' OR days_since_prior_order IS NOT NULL",
    "has_history_gap": "coalesce(order_number = 1 OR days_since_prior_order IS NOT NULL, false)",
    "has_ingest_ts": "_ingest_ts IS NOT NULL",
}
ITEM_INPUT_RULES = {
    **ITEM_RULES,
    "positive_keys": "coalesce(order_id > 0 AND product_id > 0, false)",
    "has_ingest_ts": "_ingest_ts IS NOT NULL",
}


def assess_instacart(frame: DataFrame, kind: str) -> DataFrame:
    """try_cast để lỗi kiểu vào quarantine; _raw giữ nguyên cả payload và metadata nguồn."""
    if kind not in ("orders", "items"):
        raise ValueError("Nguồn Instacart không hợp lệ")
    columns = (
        {
            "order_id": "BIGINT",
            "user_id": "BIGINT",
            "eval_set": "STRING",
            "order_number": "INT",
            "order_dow": "INT",
            "order_hour_of_day": "INT",
            "days_since_prior_order": "DOUBLE",
        }
        if kind == "orders"
        else {"order_id": "BIGINT", "product_id": "BIGINT", "add_to_cart_order": "INT", "reordered": "INT"}
    )
    columns |= {"_ingest_ts": "TIMESTAMP_LTZ", "_file_mtime": "TIMESTAMP_LTZ"}
    raw = F.struct(*(F.col("`" + name.replace("`", "``") + "`").alias(name) for name in frame.columns))
    result = frame.select(
        *(F.expr(f"try_cast(`{name}` AS {dtype})").alias(name) for name, dtype in columns.items()),
        "_rescued_data",
        "_source_file",
        raw.alias("_raw"),
    )
    return result.withColumn("_dq_failed", dq_failed(ORDER_INPUT_RULES if kind == "orders" else ITEM_INPUT_RULES))


def canonical_headers(assessed: DataFrame) -> DataFrame:
    """Ưu tiên bản đạt DQ, rồi ingest mới nhất; giữ đại diện lỗi nếu một order không có bản hợp lệ."""
    ranking = Window.partitionBy("order_id").orderBy(
        (F.size("_dq_failed") == 0).desc(),
        F.col("_ingest_ts").desc_nulls_last(),
        F.sha2(F.to_json("_raw"), 256).asc(),
    )
    return assessed.withColumn("_rn", F.row_number().over(ranking)).where(F.col("_rn") == 1).drop("_rn")


def batch_orders(clock: DataFrame) -> DataFrame:
    """Clock đã dùng đủ split; đầu ra batch chỉ có prior/test và không mang metadata file/raw."""
    return (
        clock.where(F.col("eval_set").isin("prior", "test"))
        .select(*HEADER_COLUMNS, "order_ts_sim")
        .withColumn("source", F.lit("batch"))
        .withColumn("is_test", F.col("eval_set") == "test")
    )


def quarantine_instacart(assessed: DataFrame) -> DataFrame:
    return assessed.where(F.size("_dq_failed") > 0).select(
        "_raw.*", "_dq_failed", F.current_timestamp().alias("_quarantined_at")
    )
