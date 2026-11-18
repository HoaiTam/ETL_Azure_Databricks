"""Chạy contract của JSON đã commit trên mô hình local. Azure vẫn cần nghiệm thu riêng."""

import json
import operator
import re
from pathlib import Path
from urllib.parse import quote

import pytest

from build_retail_cdc_adf import artifacts

ADF = Path(__file__).resolve().parents[1] / "adf"
FROM = "0x00000000000000000002"
TO = "0x00000000000000000014"
RUN = "original-parent-run"
TOKEN = re.compile(r"'(?:[^']|'')*'|[A-Za-z_]\w*|-?\d+|[().,\[\]]")


def read(kind, name):
    return json.loads((ADF / kind / f"{name}.json").read_text())


def evaluate(value, functions):
    if not isinstance(value, dict) or value.get("type") != "Expression":
        return value
    tokens = TOKEN.findall(value["value"][1:])
    pos = 0

    def take(expected=None):
        nonlocal pos
        token = tokens[pos]
        pos += 1
        if expected is not None:
            assert token == expected, (token, expected, value)
        return token

    def peek():
        return tokens[pos] if pos < len(tokens) else None

    def parse():
        token = take()
        if token.startswith("'"):
            result = token[1:-1].replace("''", "'")
        elif token.lstrip("-").isdigit():
            result = int(token)
        else:
            take("(")
            args = []
            if peek() != ")":
                args.append(parse())
                while peek() == ",":
                    take(",")
                    args.append(parse())
            take(")")
            result = functions[token](*args)
        while peek() in (".", "["):
            if take() == ".":
                result = result[take()]
            else:
                result = result[parse()]
                take("]")
        return result

    result = parse()
    assert pos == len(tokens)
    return result


class RetailRun:
    def __init__(self, mode="window", rows=2, *, read_rows=None, missing_stats=False, creates_file=None, failure=None):
        self.lookup = {"mode": mode, "from_lsn": FROM if mode == "window" else None, "to_lsn": TO}
        self.rows = rows
        self.read_rows = rows if read_rows is None else read_rows
        self.creates_file = rows > 0 if creates_file is None else creates_file
        self.missing_stats = missing_stats
        self.failure = failure
        self.stage = set()
        self.landing = set()
        self.calls = []
        self.commits = []
        self.queries = []

    def run(self, pipeline="pl_ingest_retail_db", params=None):
        outputs = {}
        functions = {
            "activity": lambda name: {"output": outputs[name]},
            "pipeline": lambda: {"parameters": params or {}, "RunId": RUN},
            "json": json.loads,
            "concat": lambda *ss: "".join(ss),
            "string": str,
            "int": int,
            "and": lambda *xs: all(xs),
            "or": lambda *xs: any(xs),
            "not": operator.not_,
            "equals": operator.eq,
            "greater": operator.gt,
            "greaterOrEquals": operator.ge,
            "empty": operator.not_,
            "trim": str.strip,
            "length": len,
            "replace": str.replace,
            "startsWith": str.startswith,
            "uriComponent": lambda s: quote(s, safe=""),
        }

        def ev(value):
            return evaluate(value, functions)

        def path(ref):
            keys = ref["parameters"]
            return f"{ev(keys['folder_path'])}/{ev(keys['file_name'])}"

        def activities(nodes):
            for node in nodes:
                name, kind, props = node["name"], node["type"], node["typeProperties"]
                assert all(
                    dependency["activity"] in outputs and dependency["dependencyConditions"] == ["Succeeded"]
                    for dependency in node.get("dependsOn", [])
                )
                self.calls.append(name)
                if name == self.failure:
                    raise RuntimeError(name)
                output = {}
                if kind == "ForEach":
                    assert props["isSequential"] is True
                    for item in ev(props["items"]):
                        functions["item"] = lambda item=item: item
                        activities(props["activities"])
                elif kind == "ExecutePipeline":
                    assert props["waitOnCompletion"] is True
                    self.run(props["pipeline"]["referenceName"], {k: ev(v) for k, v in props["parameters"].items()})
                elif kind == "Lookup":
                    assert props["source"]["sqlReaderStoredProcedureName"] == "etl.usp_get_cdc_window"
                    assert props["firstRowOnly"] is True
                    assert ev(props["source"]["storedProcedureParameters"]["capture_instance"]["value"]) in (
                        "dbo_product_prices",
                        "dbo_promotions",
                    )
                    output = {"firstRow": self.lookup}
                elif kind == "Switch":
                    branches = {c["value"]: c["activities"] for c in props["cases"]}
                    activities(branches.get(ev(props["on"]), props["defaultActivities"]))
                elif kind == "IfCondition":
                    if ev(props["expression"]):
                        activities(props["ifTrueActivities"])
                elif kind == "Copy":
                    dest = path(node["outputs"][0])
                    if props["source"]["type"] == "AzureSqlSource":
                        self.queries.append(ev(props["source"]["sqlReaderQuery"]))
                        if self.creates_file:
                            self.stage.add(dest)
                        output = {"rowsRead": self.read_rows}
                        if not self.missing_stats:
                            output["rowsCopied"] = self.rows
                    else:
                        assert props["validateDataConsistency"] is True
                        assert path(node["inputs"][0]) in self.stage
                        self.landing.add(dest)
                elif kind == "GetMetadata":
                    assert props["fieldList"] == ["exists"]
                    output = {"exists": path(props["dataset"]) in self.stage | self.landing}
                elif kind == "Delete":
                    assert props["storeSettings"]["recursive"] is False
                    self.stage.remove(path(props["dataset"]))
                elif kind == "SqlServerStoredProcedure":
                    assert props["storedProcedureName"] == "etl.usp_set_cdc_watermark"
                    self.commits.append({k: ev(v["value"]) for k, v in props["storedProcedureParameters"].items()})
                elif kind == "Fail":
                    raise RuntimeError(name)
                else:
                    raise AssertionError(f"Activity chưa có trong mô hình test: {kind}")
                outputs[name] = output

        activities(read("pipelines", pipeline))


def test_assets_match_generator_and_use_sequential_deployment_contract():
    for path, expected in artifacts().items():
        assert json.loads((ADF / path).read_text()) == expected
    for name in ("pl_ingest_retail_db", "pl_cdc_one_table", "pl_cdc_publish_slice"):
        settings = read("pipeline_settings", name)
        assert settings["concurrency"] == 1
        assert all(isinstance(value, str) for value in settings["parameters"].values())
    for kind in ("parquet", "binary"):
        assert (
            read("datasets", f"ds_retail_cdc_{kind}")["properties"]["typeProperties"]["location"]["fileSystem"]
            == "landing"
        )


@pytest.mark.parametrize("mode", ["snapshot", "window"])
def test_lookup_select_publish_cleanup_and_watermark_for_both_tables(mode):
    run = RetailRun(mode)
    run.run()
    suffix = f"snapshot={TO}" if mode == "snapshot" else f"window={FROM}_{TO}"
    assert run.stage == set()
    assert run.landing == {
        f"retail_db/{table}/{suffix}/part-00000.parquet" for table in ("product_prices", "promotions")
    }
    for table, query, marker in zip(("product_prices", "promotions"), run.queries, run.commits, strict=True):
        assert marker == {
            "capture_instance": f"dbo_{table}",
            "adf_run_id": RUN,
            "mode": mode,
            "rows": 2,
            "to_lsn": TO,
            **({"from_lsn": FROM} if mode == "window" else {}),
        }
        if mode == "window":
            assert f"cdc.[fn_cdc_get_all_changes_dbo_{table}](CONVERT(binary(10), '{FROM}', 1), " in query
            assert f"CONVERT(binary(10), '{TO}', 1), N'all')" in query
            assert "[__$operation] AS _op" in query
        else:
            assert f"SELECT _lsn = '{TO}', '0x00000000000000000000' AS _seqval, 0 AS _op" in query
            assert query.endswith(f"FROM dbo.[{table}]")
    assert run.calls.index("publish_parquet") < run.calls.index("delete_staged_parquet")
    assert run.calls.index("delete_staged_parquet") < run.calls.index(f"set_watermark_{mode}")


def test_empty_mode_logs_without_any_storage_activity_or_from_lsn():
    run = RetailRun("empty", rows=0)
    run.run()
    assert run.stage == run.landing == set()
    assert len(run.commits) == 2
    assert not run.queries and "get_staged_file" not in run.calls
    assert all(c["rows"] == 0 and "from_lsn" not in c for c in run.commits)


@pytest.mark.parametrize("creates_file", [True, False])
def test_zero_row_window_advances_without_a_landing_file(creates_file):
    run = RetailRun(rows=0, creates_file=creates_file)
    run.run()
    assert run.stage == run.landing == set()
    assert len(run.commits) == 2 and all(c["rows"] == 0 and c["from_lsn"] == FROM for c in run.commits)


@pytest.mark.parametrize("failure", ["copy_window", "publish_parquet", "delete_staged_parquet", "set_watermark_window"])
def test_failed_stage_does_not_commit_and_replay_uses_same_paths(failure):
    run = RetailRun(failure=failure)
    with pytest.raises(RuntimeError, match=failure):
        run.run()
    assert run.commits == []
    run.failure = None
    run.run()
    assert len(run.landing) == len(run.commits) == 2 and not run.stage
    assert all(c["from_lsn"] == FROM and c["to_lsn"] == TO for c in run.commits)


@pytest.mark.parametrize(
    ("options", "error", "message"),
    [
        ({"read_rows": 3}, RuntimeError, "bad_publish_input"),
        ({"missing_stats": True}, KeyError, "rowsCopied"),
        ({"creates_file": False}, RuntimeError, "missing_published_file"),
        ({"mode": "unsupported"}, RuntimeError, "invalid_window_mode"),
    ],
)
def test_invalid_copy_and_lookup_results_never_publish_or_advance(options, error, message):
    run = RetailRun(**options)
    with pytest.raises(error, match=message):
        run.run()
    assert not run.commits and not run.landing


def test_publish_retry_after_delete_uses_existing_final_file_without_copy():
    run = RetailRun()
    params = {"ci": "dbo_promotions", "slice": f"window={FROM}_{TO}", "rows_read": "2", "rows_copied": "2"}
    run.landing.add(f"retail_db/promotions/{params['slice']}/part-00000.parquet")
    run.run("pl_cdc_publish_slice", params)
    assert "publish_parquet" not in run.calls and "delete_staged_parquet" not in run.calls
    params["slice"] = "snapshot=0x" + "../" + "A" * 17
    with pytest.raises(RuntimeError, match="bad_publish_input"):
        run.run("pl_cdc_publish_slice", params)
