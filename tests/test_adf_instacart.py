from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from build_instacart_adf import full_definition, render
from tests.adf_runtime import LocalADF, PipelineFailure

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests/fixtures/instacart"
MUTATIONS = {"copy_csv_to_staging", "publish_csv", "delete_staging", "write_success"}


@pytest.fixture
def runtime(tmp_path):
    engine = LocalADF(tmp_path, FIXTURE, ROOT)
    rows = {}
    for file in FIXTURE.glob("*.csv"):
        with file.open(newline="") as stream:
            rows[file.stem] = sum(1 for _ in csv.DictReader(stream))
    engine.settings["pl_ingest_instacart"]["parameters"]["expected_rows"] = json.dumps(rows)
    return engine


def walk(activities, ancestors=()):
    for activity in activities:
        yield activity, ancestors
        props = activity["typeProperties"]
        for key in ("activities", "ifTrueActivities", "ifFalseActivities"):
            yield from walk(props.get(key, []), ancestors + (activity["type"],))


def test_generated_artifacts_are_current_and_terraform_parameters_are_strings():
    artifacts = render()
    for path, expected in artifacts.items():
        assert json.loads((ROOT / path).read_text()) == expected
    for name in ("pl_ingest_instacart", "pl_ingest_instacart_table"):
        definition = full_definition(name, artifacts)["properties"]
        assert definition["concurrency"] == 1
        assert all(
            p["type"] == "String" and isinstance(p["defaultValue"], str) for p in definition["parameters"].values()
        )
        for a, ancestors in walk(definition["activities"]):
            assert not (a["type"] in {"IfCondition", "Switch"} and {"IfCondition", "Switch"}.intersection(ancestors))
            assert not (a["type"] in {"ForEach", "Until"} and {"ForEach", "Until"}.intersection(ancestors))
            assert all(d["dependencyConditions"] == ["Succeeded"] for d in a["dependsOn"])
    assert definition["activities"]


def test_source_catalog_mapping_and_copy_settings_match_data_contract():
    artifacts = render()
    source = json.loads((ROOT / "adf/instacart_sources.json").read_text())
    params = artifacts["adf/pipeline_settings/pl_ingest_instacart.json"]["parameters"]
    assert source["record"] == 2654444 and source["release"] == "2017"
    assert list(json.loads(params["expected_rows"]).values()) == [3421083, 32434489, 1384617, 49688, 134, 21]
    assert json.loads(params["tables_json"]) == list(source["tables"])
    for table, mappings in json.loads(params["mappings_json"]).items():
        with (FIXTURE / f"{table}.csv").open() as stream:
            assert (
                next(csv.reader(stream))
                == [m["source"]["name"] for m in mappings]
                == source["tables"][table]["columns"]
            )
        assert all(m["source"] == m["sink"] for m in mappings)
    parent = artifacts["adf/pipelines/pl_ingest_instacart.json"][0]
    assert parent["typeProperties"]["isSequential"]
    for a, _ in walk(artifacts["adf/pipelines/pl_ingest_instacart_table.json"]):
        if a["type"] == "Copy":
            if a["name"] == "copy_csv_to_staging":
                assert a["typeProperties"]["translator"]["type"] == "TabularTranslator"
                assert a["typeProperties"]["sink"]["formatSettings"]["quoteAllText"] is True
                assert a["typeProperties"]["enableSkipIncompatibleRow"] is False
                assert "validateDataConsistency" not in a["typeProperties"]  # HTTP không hỗ trợ.
            else:
                assert a["typeProperties"]["validateDataConsistency"] is True
                assert a["typeProperties"]["sink"]["storeSettings"]["copyBehavior"] == "PreserveHierarchy"
            assert a["policy"]["retry"] >= 3
    text = json.dumps(artifacts)
    assert "RunId" not in text and "utcNow" not in text and "compressionCodec" not in text


def test_full_fixture_publish_preserves_values_and_second_run_is_read_only(runtime):
    runtime.run()
    before = {}
    for file in FIXTURE.glob("*.csv"):
        published = runtime.root / "landing/instacart" / file.stem / "release=2017" / file.name
        with file.open(newline="") as source, published.open(newline="") as target:
            assert list(csv.reader(source)) == list(csv.reader(target))
        marker = published.parent / "_SUCCESS"
        assert marker.exists() and marker.stat().st_size == 0
        before[published] = (published.read_bytes(), published.stat().st_mtime_ns)
    assert not list((runtime.root / "landing/_staging/instacart").rglob("*.csv"))
    trace_length = len(runtime.trace)
    runtime.run()
    assert not any(name in MUTATIONS for name, _ in runtime.trace[trace_length:])
    assert all((path.read_bytes(), path.stat().st_mtime_ns) == value for path, value in before.items())


@pytest.mark.parametrize(
    "failure",
    [
        ("copy_csv_to_staging", "partial"),
        ("publish_csv", "partial"),
        ("delete_staging", "before"),
        ("write_success", "before"),
    ],
)
def test_partial_failure_never_marks_release_and_retry_recovers(runtime, failure):
    runtime.fail_at = failure
    with pytest.raises(PipelineFailure):
        runtime.run()
    assert not list(runtime.root.glob("landing/instacart/*/release=2017/_SUCCESS"))
    runtime.run()
    assert len(list(runtime.root.glob("landing/instacart/*/release=2017/_SUCCESS"))) == 6
    assert not list(runtime.root.glob("landing/_staging/**/*.csv"))


@pytest.mark.parametrize("output", [{"rowsCopied": 0, "rowsRead": 0}, {"rowsCopied": 1, "rowsRead": 999}, {}])
def test_wrong_or_missing_count_prevents_publication(runtime, output):
    runtime.copy_output = output
    with pytest.raises((PipelineFailure, KeyError)):
        runtime.run()
    assert not (runtime.root / "landing/instacart").exists()
    assert any(runtime.root.glob("landing/_staging/**/*.csv"))


def test_missing_or_nonempty_template_prevents_http_copy(runtime):
    template = runtime.root / "landing/ops/_SUCCESS"
    template.write_bytes(b"not empty")
    with pytest.raises(PipelineFailure, match="INSTACART_MARKER_TEMPLATE"):
        runtime.run()
    assert not any(name == "copy_csv_to_staging" for name, _ in runtime.trace)
    template.unlink()
    with pytest.raises(FileNotFoundError):
        runtime.run()


def test_missing_marker_after_binary_copy_is_a_pipeline_failure(runtime):
    runtime.drop_marker = True
    with pytest.raises(PipelineFailure, match="INSTACART_MISSING_MARKER"):
        runtime.run()


def test_lost_acknowledgment_after_marker_does_not_rewrite_published_table(runtime):
    runtime.fail_at = ("write_success", "after")
    with pytest.raises(PipelineFailure):
        runtime.run()
    published = runtime.root / "landing/instacart/orders/release=2017/orders.csv"
    before = (published.read_bytes(), published.stat().st_mtime_ns)
    runtime.run()
    assert sum(name == "copy_csv_to_staging" and table == "orders" for name, table in runtime.trace) == 1
    assert (published.read_bytes(), published.stat().st_mtime_ns) == before
