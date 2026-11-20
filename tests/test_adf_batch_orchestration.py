"""Kiểm hợp đồng JSON và nhánh lỗi của ADF, với kết quả dịch vụ giả lập."""

import json
from pathlib import Path

import pytest

from tests.adf_runtime import LocalADF

REPO = Path(__file__).resolve().parents[1]
RUN_ID = "720620b1-dcba-4d1b-83a1-67d8ade0e788"


@pytest.fixture
def adf(tmp_path):
    local = LocalADF(tmp_path, tmp_path, REPO)
    local.run_id = RUN_ID
    return local


def test_child_pipelines_wait_and_call_the_configured_jobs(adf):
    master = {activity["name"]: activity for activity in adf.pipelines["pl_master"]}
    for name, target in (("ingest_retail_db", "pl_ingest_retail_db"), ("ingest_off", "pl_ingest_off")):
        activity = master[name]
        assert activity["type"] == "ExecutePipeline"
        assert activity["typeProperties"]["pipeline"] == {"referenceName": target, "type": "PipelineReference"}
        assert activity["typeProperties"]["waitOnCompletion"] is True
        assert target in adf.pipelines

    (crawler,) = adf.pipelines["pl_ingest_off"]
    batch = master["run_batch_medallion"]
    for pipeline, activity, parameter in (
        ("pl_ingest_off", crawler, "off_job_id"),
        ("pl_master", batch, "batch_job_id"),
    ):
        settings = adf.settings[pipeline]
        assert settings["concurrency"] == 1
        assert all(isinstance(value, str) for value in settings["parameters"].values())
        assert settings["parameters"][parameter] == ""
        assert activity["type"] == "DatabricksJob"
        assert activity["linkedServiceName"] == {"referenceName": "ls_dbx", "type": "LinkedServiceReference"}
        assert activity["policy"]["retry"] == 0
        assert settings["parameters"]["off_source"] == "bronze"
        context = {"parameters": settings["parameters"] | {parameter: "12345"}}
        assert adf.value(activity["typeProperties"]["jobId"], context) == "12345"

    context = {"parameters": adf.settings["pl_master"]["parameters"]}
    assert adf.value(batch["typeProperties"]["jobParameters"], context) == {"adf_run_id": RUN_ID}


@pytest.mark.parametrize("source", ["bronze", "landing"])
def test_master_passes_bootstrap_source_to_crawler(adf, source):
    master = {activity["name"]: activity for activity in adf.pipelines["pl_master"]}
    context = {"parameters": {"off_source": source}}
    child = adf.value(master["ingest_off"]["typeProperties"]["parameters"], context)
    (crawler,) = adf.pipelines["pl_ingest_off"]
    assert adf.value(crawler["typeProperties"]["jobParameters"], {"parameters": child}) == {"source": source}


def simulate(activities, adf, context, failures):
    statuses = {}
    for activity in activities:
        name = activity["name"]
        if not all(
            statuses[dependency["activity"]] in dependency["dependencyConditions"]
            or (
                "Completed" in dependency["dependencyConditions"]
                and statuses[dependency["activity"]] in {"Succeeded", "Failed"}
            )
            for dependency in activity.get("dependsOn", [])
        ):
            statuses[name] = "Skipped"
            continue

        kind = activity["type"]
        properties = activity["typeProperties"]
        failed = name in failures or kind == "Fail"
        if kind == "IfCondition" and adf.value(properties["expression"], context):
            children = simulate(properties["ifTrueActivities"], adf, context, failures)
            failed = "Failed" in children.values()
            statuses.update(children)
        if kind == "ExecutePipeline" and properties["pipeline"]["referenceName"] == "pl_ingest_off":
            children = simulate(adf.pipelines["pl_ingest_off"], adf, context, failures)
            failed = "Failed" in children.values()
            statuses.update(children)
        statuses[name] = "Failed" if failed else "Succeeded"
    return statuses


@pytest.mark.parametrize(
    ("failures", "vault", "off", "batch", "notify", "posted"),
    [
        (set(), "team-vault", "Succeeded", "Succeeded", "Skipped", None),
        ({"ingest_retail_db"}, "team-vault", "Skipped", "Skipped", "Succeeded", "Succeeded"),
        ({"run_off_crawler"}, "team-vault", "Failed", "Skipped", "Succeeded", "Succeeded"),
        ({"run_batch_medallion"}, "  ", "Succeeded", "Failed", "Succeeded", None),
        (
            {"run_batch_medallion", "post_failure_notification"},
            "team-vault",
            "Succeeded",
            "Failed",
            "Failed",
            "Failed",
        ),
        (
            {"run_batch_medallion", "read_failure_webhook"},
            "team-vault",
            "Succeeded",
            "Failed",
            "Failed",
            "Skipped",
        ),
    ],
)
def test_failure_gates_batch_and_keeps_master_failed(adf, failures, vault, off, batch, notify, posted):
    context = {"parameters": {"failure_webhook_vault": vault}}
    statuses = simulate(adf.pipelines["pl_master"], adf, context, failures)
    assert statuses["ingest_off"] == off
    assert statuses["run_batch_medallion"] == batch
    assert statuses["notify_failure"] == notify
    assert statuses.get("post_failure_notification") == posted
    assert statuses["fail_master"] == ("Failed" if failures else "Skipped")


def test_failure_message_uses_run_id_and_keeps_webhook_out_of_monitoring(adf):
    master = {activity["name"]: activity for activity in adf.pipelines["pl_master"]}
    read_secret, notify = master["notify_failure"]["typeProperties"]["ifTrueActivities"]
    context = {
        "parameters": {"failure_webhook_vault": " team-vault "},
        "outputs": {"read_failure_webhook": {"value": "https://example.invalid/notify"}},
    }
    retrieval = adf.value(read_secret["typeProperties"], context)
    assert retrieval["authentication"] == {"type": "MSI", "resource": "https://vault.azure.net"}
    assert retrieval["url"] == "https://team-vault.vault.azure.net/secrets/adf-batch-failure-webhook?api-version=7.5"
    assert read_secret["policy"]["secureOutput"]
    message = adf.value(notify["typeProperties"], context)
    assert message["url"] == "https://example.invalid/notify"
    assert message["turnOffAsync"] is True
    assert json.loads(message["body"]) == {
        "pipeline": "pl_master",
        "adf_run_id": RUN_ID,
        "text": f"ADF pl_master thất bại. RunId {RUN_ID}",
    }
    assert notify["policy"]["secureInput"]
    assert notify["policy"]["secureOutput"]
    assert RUN_ID in adf.value(master["fail_master"]["typeProperties"]["message"], context)
