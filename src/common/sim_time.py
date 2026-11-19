"""Thời gian nghiệp vụ giả lập dùng chung cho silver và simulator."""

from __future__ import annotations

import re
from datetime import UTC, date, datetime, timedelta
from operator import index

SIM_END_ANCHOR = "2026-03-01"


def order_ts_sim(user_id: int, hour: int, days_back: int = 0) -> datetime:
    """Trả UTC: mốc 01/03 + (user_id × 7919 mod 30) − days_back ngày.

    days_back là tổng số ngày cách đơn trước của các đơn SAU đơn đang xét.
    Đơn cuối (train/test) có days_back = 0. Không dùng đồng hồ hoặc múi giờ máy.
    """
    values = {"user_id": user_id, "hour": hour, "days_back": days_back}
    for name, value in values.items():
        if isinstance(value, bool):
            raise TypeError(f"{name} phải là số nguyên, không phải bool")
        try:
            values[name] = index(value)
        except TypeError as error:
            raise TypeError(f"{name} phải là số nguyên") from error
    user_id, hour, days_back = (values[key] for key in ("user_id", "hour", "days_back"))
    if user_id <= 0:
        raise ValueError("user_id phải lớn hơn 0")
    if not 0 <= hour <= 23:
        raise ValueError("hour phải nằm trong 0..23")
    if days_back < 0:
        raise ValueError("days_back không được âm")

    day = date.fromisoformat(SIM_END_ANCHOR) + timedelta(days=(user_id * 7919) % 30 - days_back)
    return datetime(day.year, day.month, day.day, hour, tzinfo=UTC)


# Dùng ROW_NUMBER trong CTE thay QUALIFY để chạy cùng câu SQL trên Spark 3.5 local.
# Cast cột nghiệp vụ vì Auto Loader CSV có thể trả STRING; sum(dsp) trả BIGINT,
# còn date_sub cần INT. make_timestamp_ltz chỉ định UTC, không phụ thuộc session.
ORDER_TIME_SIM_SQL = """
WITH ranked AS (
    SELECT CAST(order_id AS BIGINT) AS order_id,
           CAST(user_id AS BIGINT) AS user_id,
           eval_set,
           CAST(order_number AS INT) AS order_number,
           CAST(order_hour_of_day AS INT) AS order_hour_of_day,
           CAST(coalesce(CAST(days_since_prior_order AS DOUBLE), 0) AS INT) AS dsp{extra_fields}{ranking}
    FROM {orders_table}
), back AS (
    SELECT *, coalesce(sum(dsp) OVER (
        PARTITION BY user_id ORDER BY order_number
        ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING
    ), 0) AS days_back
    FROM ranked
    {rank_filter}
), dated AS (
    SELECT *, date_sub(
        date_add(DATE'{anchor}', CAST(pmod(user_id * 7919, 30) AS INT)),
        CAST(days_back AS INT)
    ) AS sim_date
    FROM back
)
SELECT order_id, user_id, eval_set{extra_output},
       make_timestamp_ltz(year(sim_date), month(sim_date), day(sim_date),
                          order_hour_of_day, 0, 0, 'UTC') AS order_ts_sim
FROM dated
"""


def order_time_sim_sql(orders_table: str, *, canonical: bool = False) -> str:
    """Dựng SELECT từ bảng bronze/view local, gồm đủ prior/train/test và _ingest_ts.

    Tên nguồn nhận 1–3 phần chưa quote, ví dụ instacart.p2_bronze.orders.
    Chỉ nhận identifier; không nhận đường dẫn file, biểu thức hoặc câu SQL.
    Với canonical=True, nguồn đã qua DQ và duy nhất theo order_id, giữ cột header cho batch.
    """
    parts = orders_table.split(".")
    if not 1 <= len(parts) <= 3 or any(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", part) is None for part in parts):
        raise ValueError("orders_table phải là tên bảng gồm 1–3 identifier chưa quote")
    ranking = (
        ", row_number() OVER (PARTITION BY CAST(order_id AS BIGINT) ORDER BY _ingest_ts DESC) AS rn"
        if not canonical
        else ""
    )
    return ORDER_TIME_SIM_SQL.format(
        orders_table=".".join(f"`{part}`" for part in parts),
        anchor=SIM_END_ANCHOR,
        extra_fields=", order_dow, days_since_prior_order, _ingest_ts" if canonical else "",
        ranking=ranking,
        rank_filter="" if canonical else "WHERE rn = 1",
        extra_output=", order_number, order_dow, order_hour_of_day, days_since_prior_order, _ingest_ts"
        if canonical
        else "",
    )
