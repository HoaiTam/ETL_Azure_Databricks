"""Chạy contract của JSON đã commit trên mô hình local. Azure vẫn cần nghiệm thu riêng."""

import json
from pathlib import Path

import pytest

from build_retail_cdc_adf import artifacts
from tests.adf_runtime import LocalADF

ADF = Path(__file__).resolve().parents[1] / "adf"
FROM = "0x00000000000000000002"
TO = "0x00000000000000000014"
RUN = "original-parent-run"


def read(kind, name):
    return json.loads((ADF / kind / f"{name}.json").read_text())


class RetailRun(LocalADF):
    def __init__(
        self,
        tmp_path,
        mode="window",
        rows=2,
        *,
        read_rows=None,
        missing_stats=False,
        creates_file=None,
        failure=None,
    ):
        super().__init__(root=tmp_path, source=tmp_path, repository=ADF.parent)
        self.run_id = RUN
        self.lookup = {"mode": mode, "from_lsn": FROM if mode == "window" else None, "to_lsn": TO}
        self.rows = rows
        self.read_rows = rows if read_rows is None else read_rows
        self.creates_file = rows > 0 if creates_file is None else creates_file
        self.missing_stats = missing_stats
        self.fail_at = (failure, "before") if failure else None
        self.commits = []
        self.queries = []

    @property
    def calls(self):
        return [name for name, _ in self.trace]

    @property
    def stage(self):
        return set((self.root / "landing/_staging/retail_db").glob("*/*"))

    @property
    def landing(self):
        return {str(file.relative_to(self.root / "landing")) for file in self._landing_files()}

    def _landing_files(self):
        return (self.root / "landing/retail_db").rglob("*.parquet")

    def run(self, pipeline="pl_ingest_retail_db", **parameters):
        return super().run(pipeline, **parameters)

    def sql_activity(self, activity, context):
        kind, props = activity["type"], activity["typeProperties"]
        if kind == "Lookup":
            sql = props["source"]
            assert sql["sqlReaderStoredProcedureName"] == "etl.usp_get_cdc_window"
            assert props["firstRowOnly"] is True
            assert self.value(sql["storedProcedureParameters"]["capture_instance"]["value"], context) in (
                "dbo_product_prices",
                "dbo_promotions",
            )
            return {"firstRow": self.lookup}
        if kind == "Copy":
            self.queries.append(self.value(props["source"]["sqlReaderQuery"], context))
            dest, _ = self.dataset(activity["outputs"][0], context)
            dest.parent.mkdir(parents=True, exist_ok=True)
            if self.creates_file:
                dest.write_bytes(b"PAR1")
            output = {"rowsRead": self.read_rows}
            if not self.missing_stats:
                output["rowsCopied"] = self.rows
            return output
        assert props["storedProcedureName"] == "etl.usp_set_cdc_watermark"
        self.commits.append(
            {key: self.value(value["value"], context) for key, value in props["storedProcedureParameters"].items()}
        )
        return {}


def test_assets_match_generator_and_use_sequential_deployment_contract():
    for path, expected in artifacts().items():
        assert json.loads((ADF / path).read_text()) == expected
    for name in ("pl_ingest_retail_db", "pl_cdc_one_table", "pl_cdc_publish_slice"):
        settings = read("pipeline_settings", name)
        assert settings["concurrency"] == 1
        assert all(isinstance(value, str) for value in settings["parameters"].values())
    props = read("datasets", "ds_retail_cdc_parquet")["properties"]["typeProperties"]
    assert props["location"]["fileSystem"] == "landing"
    assert not (ADF / "datasets/ds_retail_cdc_binary.json").exists()


@pytest.mark.parametrize("mode", ["snapshot", "window"])
def test_lookup_select_publish_cleanup_and_watermark_for_both_tables(tmp_path, mode):
    run = RetailRun(tmp_path, mode)
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


def test_empty_mode_logs_without_any_storage_activity_or_from_lsn(tmp_path):
    run = RetailRun(tmp_path, "empty", rows=0)
    run.run()
    assert run.stage == run.landing == set()
    assert len(run.commits) == 2
    assert not run.queries and "get_staged_file" not in run.calls
    assert all(c["rows"] == 0 and "from_lsn" not in c for c in run.commits)


@pytest.mark.parametrize("creates_file", [True, False])
def test_zero_row_window_advances_without_a_landing_file(tmp_path, creates_file):
    run = RetailRun(tmp_path, rows=0, creates_file=creates_file)
    run.run()
    assert run.stage == run.landing == set()
    assert len(run.commits) == 2 and all(c["rows"] == 0 and c["from_lsn"] == FROM for c in run.commits)


@pytest.mark.parametrize("failure", ["copy_window", "publish_parquet", "delete_staged_parquet", "set_watermark_window"])
def test_failed_stage_does_not_commit_and_replay_uses_same_paths(tmp_path, failure):
    run = RetailRun(tmp_path, failure=failure)
    with pytest.raises(RuntimeError, match=failure):
        run.run()
    assert run.commits == []
    run.run()
    assert len(run.landing) == len(run.commits) == 2 and not run.stage
    assert all(c["from_lsn"] == FROM and c["to_lsn"] == TO for c in run.commits)


@pytest.mark.parametrize(
    ("options", "error", "message"),
    [
        ({"read_rows": 3}, RuntimeError, "RetailCdcContract"),
        ({"missing_stats": True}, KeyError, "rowsCopied"),
        ({"creates_file": False}, RuntimeError, "RetailCdcContract"),
        ({"mode": "unsupported"}, RuntimeError, "RetailCdcContract"),
    ],
)
def test_invalid_copy_and_lookup_results_never_publish_or_advance(tmp_path, options, error, message):
    run = RetailRun(tmp_path, **options)
    with pytest.raises(error, match=message):
        run.run()
    assert not run.commits and not run.landing


def test_publish_retry_after_delete_uses_existing_final_file_without_copy(tmp_path):
    run = RetailRun(tmp_path)
    params = {"ci": "dbo_promotions", "slice": f"window={FROM}_{TO}", "rows_read": "2", "rows_copied": "2"}
    published = tmp_path / f"landing/retail_db/promotions/{params['slice']}/part-00000.parquet"
    published.parent.mkdir(parents=True)
    published.write_bytes(b"PAR1")
    run.run("pl_cdc_publish_slice", **params)
    assert "publish_parquet" not in run.calls and "delete_staged_parquet" not in run.calls
    params["slice"] = "snapshot=0x" + "../" + "A" * 17
    with pytest.raises(RuntimeError, match="RetailCdcContract"):
        run.run("pl_cdc_publish_slice", **params)
