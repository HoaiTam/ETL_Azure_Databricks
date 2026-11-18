"""Kiểm tra DataFrame smoke bằng Spark local; không thay cho nghiệm thu LDP trên dev."""

import runpy
import sys
from pathlib import Path
from types import SimpleNamespace


def test_bootstrap_is_private_and_uses_pipeline_configuration(spark, monkeypatch):
    definitions = []

    def materialized_view(**kwargs):
        definitions.append(kwargs)
        return lambda function: function

    monkeypatch.setitem(sys.modules, "pyspark.pipelines", SimpleNamespace(materialized_view=materialized_view))
    config = {
        "catalog": "test_catalog",
        "bronze": "alice_bronze",
        "silver": "alice_silver",
        "gold": "alice_gold",
        "ops": "alice_ops",
        "landing_root": "/Volumes/test_catalog/landing_meta/raw",
        "as_of_date": "2026-03-30",
    }
    previous = {key: spark.conf.get(key, None) for key in config}
    try:
        for key, value in config.items():
            spark.conf.set(key, value)
        source = Path(__file__).resolve().parents[1] / "src/pipelines/batch/bootstrap.py"
        namespace = runpy.run_path(str(source))
        result = namespace["batch_bootstrap"]().collect()
        assert definitions[0]["private"] is True
        assert len(result) == 1
        assert result[0].asDict() == config
    finally:
        for key, value in previous.items():
            if value is None:
                spark.conf.unset(key)
            else:
                spark.conf.set(key, value)
