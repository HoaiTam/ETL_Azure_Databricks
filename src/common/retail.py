"""Chuẩn hóa CDC retail, phân loại DQ và loại bản sao trước AUTO CDC; dùng được trên Spark local."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType

from common.dq import PRICE_RULES, PROMO_RULES, dq_failed

PRICE_COLUMNS = ["product_id", "unit_price", "currency", "updated_at", "_op", "_lsn", "_seqval"]
PROMO_COLUMNS = [
    "promo_id",
    "product_id",
    "promo_type",
    "discount_pct",
    "start_date",
    "end_date",
    "updated_at",
    "_op",
    "_lsn",
    "_seqval",
]

# Delete chỉ cần khóa/thứ tự; không để payload business rỗng chặn một tombstone hợp lệ.
PRICE_CHANGE_RULES = {
    name: condition if name == "has_product_id" else f"(_op = 1) OR ({condition})"
    for name, condition in PRICE_RULES.items()
}
PROMO_CHANGE_RULES = {name: f"(_op = 1) OR ({condition})" for name, condition in PROMO_RULES.items()}
TRANSPORT_RULES = {
    "valid_operation": "coalesce(_op IN (0, 1, 2, 4), false)",
    "has_lsn": "_lsn IS NOT NULL",
    "has_seqval": "_seqval IS NOT NULL",
    "no_rescued": "_rescued_data IS NULL",
}
PRICE_CHANGE_RULES |= {
    **TRANSPORT_RULES,
    "has_updated_at": "updated_at IS NOT NULL",
    "no_price_delete": "coalesce(_op <> 1, true)",
}
PROMO_CHANGE_RULES |= {
    **TRANSPORT_RULES,
    "has_promo_id": "coalesce(length(trim(promo_id)) > 0, false)",
    "has_product_id": "(_op = 1) OR (product_id IS NOT NULL)",
    "has_updated_at": "(_op = 1) OR (updated_at IS NOT NULL)",
    "has_promo_type": "(_op = 1) OR coalesce(length(trim(promo_type)) > 0, false)",
}


def _log_position(frame: DataFrame, name: str):
    """LSN/seqval của SQL Server là binary(10); nhận cả binary và hex 0x + 20 chữ số."""
    value = F.col(name)
    if isinstance(frame.schema[name].dataType, BinaryType):
        return F.when(F.length(value) == 10, value)
    text = value.cast("string")
    return F.when(text.rlike(r"^0[xX][0-9a-fA-F]{20}$"), F.unhex(F.substring(text, 3, 20)))


def assess_changes(frame: DataFrame, kind: str) -> DataFrame:
    """Giữ raw payload, try_cast an toàn dưới ANSI; session phải dùng UTC như pipeline."""
    if kind not in ("prices", "promotions"):
        raise ValueError("Nguồn retail không hợp lệ")
    common = {
        "product_id": "INT",
        "updated_at": "TIMESTAMP_LTZ",
        "_op": "INT",
    }
    types = (
        {**common, "unit_price": "DECIMAL(10,2)", "currency": "STRING"}
        if kind == "prices"
        else {
            **common,
            "promo_id": "STRING",
            "promo_type": "STRING",
            "discount_pct": "INT",
            "start_date": "DATE",
            "end_date": "DATE",
        }
    )
    raw = F.struct(*(F.col("`" + name.replace("`", "``") + "`").alias(name) for name in frame.columns))
    result = frame.select(
        *(F.expr(f"try_cast(`{name}` AS {dtype})").alias(name) for name, dtype in types.items()),
        _log_position(frame, "_lsn").alias("_lsn"),
        _log_position(frame, "_seqval").alias("_seqval"),
        "_rescued_data",
        raw.alias("_raw"),
    )
    # Before-image không phải trạng thái mới. Giữ op NULL/không hợp lệ để đưa vào quarantine.
    result = result.where(F.col("_op").isNull() | (F.col("_op") != 3))
    rules = PRICE_CHANGE_RULES if kind == "prices" else PROMO_CHANGE_RULES
    return result.withColumn("_dq_failed", dq_failed(rules))


def deduplicate_changes(validated: DataFrame, kind: str) -> DataFrame:
    """Đầu vào đã qua DQ. Chỉ gộp bản sao cùng payload, không che xung đột cùng sequence."""
    if kind not in ("prices", "promotions"):
        raise ValueError("Nguồn retail không hợp lệ")
    columns = PRICE_COLUMNS if kind == "prices" else PROMO_COLUMNS
    # Giá dùng business time; re-snapshot cùng giá có thể mang LSN khác mà không phải phiên bản mới.
    changes = validated.select(*columns)
    if kind == "promotions":
        return changes.dropDuplicates(columns)
    # Snapshot, insert và after-image cùng payload ở một business time đều là một upsert.
    identity = [name for name in columns if name not in ("_op", "_lsn", "_seqval")]
    return (
        changes.withColumn("_is_delete", F.col("_op") == 1).dropDuplicates([*identity, "_is_delete"]).drop("_is_delete")
    )


def quarantine_changes(assessed: DataFrame) -> DataFrame:
    """Giữ nguyên cả giá trị sai và metadata gốc, kèm mọi tên luật lỗi."""
    return assessed.where(F.size("_dq_failed") > 0).select(
        "_raw.*", "_dq_failed", F.current_timestamp().alias("_quarantined_at")
    )
