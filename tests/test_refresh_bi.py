"""Kiểm audit Delta, nhánh bỏ qua, API giả lập và repair theo cùng run ID."""

from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
import requests
import yaml

from tasks.refresh_bi import RefreshResult, pipeline_run_record, run_task, submit_refresh

ROOT = Path(__file__).resolve().parents[1]
CHECK_DATE = "2026-03-30"


def test_builds_audit_record_with_adf_and_databricks_ids():
    checks = {"checks_total": 2, "checks_passed": 2, "rows_processed": 45, "rows_metric": "g4.fact.rows"}
    record = pipeline_run_record(
        "run-adf", "run-db", 1774828800000, checks, RefreshResult("skipped", "refresh_disabled")
    )
    assert (record["adf_run_id"], record["run_id"], record["status"]) == ("run-adf", "run-db", "succeeded")
    assert record["rows_processed"] == 45
    assert record["refresh_reason"] == "refresh_disabled"


def test_batch_job_dependencies_gate_refresh_and_audit():
    job = yaml.safe_load((ROOT / "bundles/resources/jobs_batch.yml").read_text())["resources"]["jobs"][
        "batch_medallion"
    ]
    tasks = {task["task_key"]: task for task in job["tasks"]}
    refresh, audit = tasks["refresh_bi"], tasks["record_failed_batch"]
    assert refresh["run_if"] == "ALL_SUCCESS"
    assert refresh["depends_on"] == [{"task_key": "reconcile"}]
    assert audit["run_if"] == "AT_LEAST_ONE_FAILED"
    assert {"pipeline_batch", "reconcile"} <= {item["task_key"] for item in audit["depends_on"]}
    assert "{{job.parameters.adf_run_id}}" in refresh["spark_python_task"]["parameters"]


@pytest.fixture
def ops(spark):
    name = "test_bi_" + uuid4().hex
    spark.sql(f"CREATE DATABASE `{name}`").collect()
    spark.sql(f"""
      CREATE TABLE `{name}`.reconciliation
        (check_date DATE, check_name STRING, actual BIGINT, passed BOOLEAN, run_id STRING)
      USING DELTA
    """).collect()
    args = SimpleNamespace(
        catalog="spark_catalog",
        ops_schema=name,
        run_id="101",
        adf_run_id="adf-101",
        start_ms=1774828800000,
        check_date=CHECK_DATE,
        mode="refresh",
        enabled="false",
    )

    def put(metric, passed):
        spark.sql(
            f"INSERT INTO `{name}`.reconciliation VALUES (DATE'{CHECK_DATE}', :metric, 42L, :passed, '101')",
            args={"metric": metric, "passed": passed},
        ).collect()

    def recorded():
        return spark.table(f"`{name}`.pipeline_runs").collect()

    try:
        yield args, put, recorded
    finally:
        spark.sql(f"DROP DATABASE `{name}` CASCADE").collect()


def test_failure_audit_and_successful_repair_write_one_row_without_calling_api(spark, ops):
    args, put, recorded = ops

    def unexpected(*args, **kwargs):
        pytest.fail("API hoặc secret không được gọi trong đường audit/disabled")

    args.mode = "audit_failed"
    result = run_task(spark, args, load_secrets=unexpected, post=unexpected)
    assert result.reason == "upstream_failed"
    (row,) = recorded()
    assert row.status == "failed" and row.rows_processed is None

    args.mode = "refresh"
    put("silver.prior.rows", True)
    run_task(spark, args, load_secrets=unexpected, post=unexpected)
    (row,) = recorded()
    assert (row.adf_run_id, row.run_id, row.status) == ("adf-101", "101", "succeeded")
    assert (row.rows_processed, row.rows_metric, row.refresh_reason) == (42, "silver.prior.rows", "refresh_disabled")

    args.enabled = "true"
    assert run_task(spark, args, load_secrets=unexpected, post=unexpected).reason == "gold_reconciliation_missing"


def fake_secrets():
    return {key: key for key in ("tenant-id", "client-id", "client-secret", "workspace-id", "dataset-id")}


def test_accepted_refresh_is_not_submitted_again_on_repair(spark, ops):
    args, put, recorded = ops
    args.enabled = "true"
    put("g4.fact.rows", True)
    calls = []

    def post(url, **kwargs):
        calls.append((url, kwargs))
        if "microsoftonline.com" in url:
            return SimpleNamespace(status_code=200, json=lambda: {"access_token": "fake-token"})
        assert len(recorded()) == 1 and recorded()[0].refresh_status == "submitting"
        return SimpleNamespace(status_code=202, headers={"x-ms-request-id": "pbi-request"})

    result = run_task(spark, args, load_secrets=fake_secrets, post=post)
    assert result == RefreshResult("requested", "power_bi_accepted", "pbi-request")
    assert run_task(spark, args, load_secrets=fake_secrets, post=post) == result
    assert len(calls) == 2
    assert calls[1][0].endswith("/groups/workspace-id/datasets/dataset-id/refreshes")
    (row,) = recorded()
    assert row.refresh_request_id == "pbi-request" and row.rows_metric == "g4.fact.rows"


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        (403, RefreshResult("skipped", "power_bi_permission_denied")),
        (429, RefreshResult("failed", "power_bi_http_429")),
        (503, RefreshResult("submission_unknown", "power_bi_http_503")),
    ],
)
def test_refresh_api_statuses_are_distinct(code, expected):
    assert submit_refresh(fake_secrets(), "fake", lambda *args, **kw: SimpleNamespace(status_code=code)) == expected


def test_transport_timeout_does_not_claim_refresh_failed_or_completed():
    def post(*args, **kwargs):
        raise requests.Timeout

    assert submit_refresh(fake_secrets(), "fake", post) == RefreshResult(
        "submission_unknown", "request_transport_error"
    )
