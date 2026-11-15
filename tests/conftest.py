from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("instacart-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
