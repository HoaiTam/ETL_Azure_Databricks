"""Đồ thị smoke tối thiểu cho AD-DE-08, không công bố bảng nghiệp vụ giả vào UC."""

from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@dp.materialized_view(
    name="_batch_bootstrap",
    private=True,
    comment="Kiểm tra pipeline nhận đủ cấu hình; chỉ lưu trong phạm vi pipeline.",
)
def batch_bootstrap():
    """Một dòng cấu hình giúp update/event log chạy được trước khi có AD-DE-09."""
    spark = SparkSession.active()
    keys = ("catalog", "bronze", "silver", "gold", "ops", "landing_root", "as_of_date")
    return spark.range(1).select(*(F.lit(spark.conf.get(key)).alias(key) for key in keys))
