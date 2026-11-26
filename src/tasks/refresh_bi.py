"""Làm mới mô hình Power BI và lưu nhật ký theo Databricks job run ID."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from uuid import UUID

import requests

SECRET_KEYS = ("tenant-id", "client-id", "client-secret", "workspace-id", "dataset-id")
UNSAFE_TO_REPEAT = {"submitting", "submission_unknown", "requested"}
SCOPE = "https://analysis.windows.net/powerbi/api/.default"


@dataclass(frozen=True)
class RefreshResult:
    status: str
    reason: str
    request_id: str | None = None


def pipeline_run_record(
    adf_run_id: str, run_id: str, start_ms: int, checks: dict, result: RefreshResult, *, upstream_failed: bool = False
) -> dict:
    if not adf_run_id.strip() or not run_id.strip() or start_ms <= 0:
        raise ValueError("Thiếu định danh hoặc thời điểm bắt đầu job")
    status = "succeeded"
    if upstream_failed or result.status in {"failed", "submission_unknown"}:
        status = "failed"
    elif result.status == "submitting":
        status = "running"
    return {
        "adf_run_id": adf_run_id,
        "run_id": run_id,
        "start_ms": start_ms,
        "status": status,
        "refresh_status": result.status,
        "refresh_reason": result.reason,
        "refresh_request_id": result.request_id,
        **checks,
    }


def power_bi_token(secrets, post) -> str | None:
    try:
        response = post(
            f"https://login.microsoftonline.com/{secrets['tenant-id']}/oauth2/v2.0/token",
            data={
                "grant_type": "client_credentials",
                "client_id": secrets["client-id"],
                "client_secret": secrets["client-secret"],
                "scope": SCOPE,
            },
            timeout=30,
            allow_redirects=False,
        )
        if response.status_code == 200:
            body = response.json()
            token = body.get("access_token") if isinstance(body, dict) else None
            return token if isinstance(token, str) and token else None
    except (requests.RequestException, ValueError):
        return None
    return None


def submit_refresh(secrets, token: str, post=requests.post) -> RefreshResult:
    url = (
        "https://api.powerbi.com/v1.0/myorg/groups/"
        f"{secrets['workspace-id']}/datasets/{secrets['dataset-id']}/refreshes"
    )
    try:
        response = post(
            url,
            headers={"Authorization": f"Bearer {token}"},
            json={"notifyOption": "NoNotification"},
            timeout=30,
            allow_redirects=False,
        )
    except requests.RequestException:
        return RefreshResult("submission_unknown", "request_transport_error")
    status = response.status_code
    if status == 202:
        return RefreshResult("requested", "power_bi_accepted", response.headers.get("x-ms-request-id"))
    if status == 403:
        return RefreshResult("skipped", "power_bi_permission_denied")
    if status >= 500 or 300 <= status < 400:
        return RefreshResult("submission_unknown", f"power_bi_http_{status}")
    return RefreshResult("failed", f"power_bi_http_{status}")


def qualified_table(catalog: str, schema: str, table: str) -> str:
    return ".".join("`" + part.replace("`", "``") + "`" for part in (catalog, schema, table))


def read_checks(spark, table: str, run_id: str, check_date: str) -> dict:
    checks = {"checks_total": 0, "checks_passed": 0, "rows_processed": None, "rows_metric": None}
    if not spark.catalog.tableExists(table):
        return checks
    row = spark.sql(
        """
        SELECT count(*) AS checks_total, count_if(passed) AS checks_passed,
          max(CASE WHEN check_name = 'g4.fact.rows' THEN actual END) AS fact_rows,
          max(CASE WHEN check_name = 'silver.prior.rows' THEN actual END) AS prior_rows
        FROM IDENTIFIER(:checks_table)
        WHERE run_id = :run_id AND check_date = CAST(:check_date AS DATE)
        """,
        args={"checks_table": table, "run_id": run_id, "check_date": check_date},
    ).first()
    checks.update(checks_total=row.checks_total, checks_passed=row.checks_passed or 0)
    if row.fact_rows is not None:
        checks.update(rows_processed=row.fact_rows, rows_metric="g4.fact.rows")
    elif row.prior_rows is not None:
        checks.update(rows_processed=row.prior_rows, rows_metric="silver.prior.rows")
    return checks


def init_run_log(spark, table: str) -> None:
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS IDENTIFIER(:runs_table) (
          run_id STRING NOT NULL, adf_run_id STRING NOT NULL, started_at TIMESTAMP NOT NULL,
          recorded_at TIMESTAMP NOT NULL, status STRING NOT NULL, refresh_status STRING NOT NULL,
          refresh_reason STRING NOT NULL, refresh_request_id STRING,
          checks_total BIGINT NOT NULL, checks_passed BIGINT NOT NULL, rows_processed BIGINT, rows_metric STRING
        ) USING DELTA
        """,
        args={"runs_table": table},
    ).collect()


def save_run(spark, table: str, record: dict) -> None:
    spark.sql(
        """
        MERGE INTO IDENTIFIER(:runs_table) AS target
        USING (
          SELECT :run_id AS run_id, :adf_run_id AS adf_run_id, timestamp_millis(:start_ms) AS started_at,
            current_timestamp() AS recorded_at, :status AS status, :refresh_status AS refresh_status,
            :refresh_reason AS refresh_reason, CAST(:refresh_request_id AS STRING) AS refresh_request_id,
            CAST(:checks_total AS BIGINT) AS checks_total, CAST(:checks_passed AS BIGINT) AS checks_passed,
            CAST(:rows_processed AS BIGINT) AS rows_processed, CAST(:rows_metric AS STRING) AS rows_metric
        ) AS source
        ON target.run_id = source.run_id
        WHEN MATCHED THEN UPDATE SET
          adf_run_id = coalesce(nullif(source.adf_run_id, 'manual'), target.adf_run_id),
          started_at = source.started_at,
          recorded_at = source.recorded_at,
          status = source.status,
          refresh_status = source.refresh_status,
          refresh_reason = source.refresh_reason,
          refresh_request_id = source.refresh_request_id,
          checks_total = source.checks_total,
          checks_passed = source.checks_passed,
          rows_processed = source.rows_processed,
          rows_metric = source.rows_metric
        WHEN NOT MATCHED THEN INSERT *
        """,
        args={"runs_table": table, **record},
    ).collect()


def load_power_bi_secrets() -> dict:
    # Chỉ truy xuất scope khi cấu hình bật API và các check đã đạt.
    try:
        from databricks.sdk.runtime import dbutils

        secrets = {key: dbutils.secrets.get("kv", f"power-bi-{key}").strip() for key in SECRET_KEYS}
        for key in ("tenant-id", "client-id", "workspace-id", "dataset-id"):
            secrets[key] = str(UUID(secrets[key]))
        if not secrets["client-secret"]:
            raise ValueError("Thiếu thông tin xác thực")
    except Exception:
        raise ValueError("power_bi_credentials_unavailable") from None
    return secrets


def run_task(spark, args, load_secrets=load_power_bi_secrets, post=requests.post) -> RefreshResult:
    table = qualified_table(args.catalog, args.ops_schema, "pipeline_runs")
    checks_table = qualified_table(args.catalog, args.ops_schema, "reconciliation")
    init_run_log(spark, table)
    previous = spark.sql(
        """
        SELECT refresh_status, refresh_reason, refresh_request_id FROM IDENTIFIER(:runs_table)
        WHERE run_id = :run_id
        """,
        args={"runs_table": table, "run_id": args.run_id},
    ).first()
    if previous and previous.refresh_status in UNSAFE_TO_REPEAT:
        if previous.refresh_status != "requested":
            raise RuntimeError("Power BI có thể đã nhận refresh. Kiểm tra lịch sử trước khi gửi lại.")
        return RefreshResult(previous.refresh_status, previous.refresh_reason, previous.refresh_request_id)

    checks = read_checks(spark, checks_table, args.run_id, args.check_date)

    def save(result: RefreshResult, *, upstream_failed: bool = False) -> RefreshResult:
        record = pipeline_run_record(
            args.adf_run_id, args.run_id, args.start_ms, checks, result, upstream_failed=upstream_failed
        )
        save_run(spark, table, record)
        return result

    if args.mode == "audit_failed":
        return save(RefreshResult("skipped", "upstream_failed"), upstream_failed=True)
    if not checks["checks_total"] or checks["checks_passed"] != checks["checks_total"]:
        save(RefreshResult("skipped", "reconciliation_not_green"), upstream_failed=True)
        raise RuntimeError("Đối soát chưa đạt cho job run này")
    if args.enabled == "false":
        return save(RefreshResult("skipped", "refresh_disabled"))
    if checks["rows_metric"] != "g4.fact.rows":
        return save(RefreshResult("skipped", "gold_reconciliation_missing"))

    try:
        secrets = load_secrets()
    except ValueError:
        save(RefreshResult("failed", "power_bi_credentials_unavailable"))
        raise RuntimeError("Chưa cấu hình thông tin xác thực Power BI") from None
    token = power_bi_token(secrets, post)
    if token is None:
        save(RefreshResult("failed", "power_bi_token_failed"))
        raise RuntimeError("Không lấy được token Power BI")

    # Ghi ý định trước POST để repair không gửi lặp khi kết quả của lời gọi chưa xác định.
    save(RefreshResult("submitting", "submission_started"))
    result = save(submit_refresh(secrets, token, post))
    if result.status in {"failed", "submission_unknown"}:
        raise RuntimeError(f"Power BI refresh: {result.reason}")
    return result


def main() -> None:
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--ops-schema", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--adf-run-id", required=True)
    parser.add_argument("--start-ms", type=int, required=True)
    parser.add_argument("--check-date", required=True)
    parser.add_argument("--enabled", choices=("true", "false"), default="false")
    parser.add_argument("--mode", choices=("refresh", "audit_failed"), default="refresh")
    args = parser.parse_args()
    run_task(SparkSession.builder.getOrCreate(), args)


if __name__ == "__main__":
    main()
