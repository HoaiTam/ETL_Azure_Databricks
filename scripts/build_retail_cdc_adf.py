#!/usr/bin/env python3
"""Tái tạo activities, settings và dataset cho CDC retail theo LSN."""

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "adf"
TABLES = ("dbo_product_prices", "dbo_promotions")
MODES = ("empty", "snapshot", "window")
FILE = "part-00000.parquet"
LOOKUP = "activity('lk_window').output.firstRow"
CI = "pipeline().parameters.ci"
READ = "pipeline().parameters.rows_read"
COPIED = "pipeline().parameters.rows_copied"
SLICE = "pipeline().parameters.slice"
COLUMNS = {
    "dbo_product_prices": "[product_id], [unit_price], [currency], [updated_at]",
    "dbo_promotions": "[promo_id], [product_id], [promo_type], [discount_pct], [start_date], [end_date], [updated_at]",
}
RAW_CDC = "[__$start_lsn], [__$seqval], [__$operation], [__$update_mask]"
SNAPSHOT_CDC = (
    "CAST(NULL AS binary(10)) AS [__$start_lsn], CAST(NULL AS binary(10)) AS [__$seqval], "
    "CAST(NULL AS int) AS [__$operation], CAST(NULL AS varbinary(128)) AS [__$update_mask]"
)


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def expr(body):
    return {"type": "Expression", "value": "@" + body}


def select_ci(mapping):
    # Dùng danh sách cố định cho tên bảng/hàm và cột. ci chỉ chọn khóa, không ghép vào SQL.
    return f"json({literal(json.dumps(mapping, separators=(',', ':')))})[{CI}]"


def proc_param(value, kind="String"):
    return {"value": value, "type": kind}


def link(name):
    return {"referenceName": name, "type": "LinkedServiceReference"}


def dataset_ref(name, **parameters):
    ref = {"referenceName": name, "type": "DatasetReference"}
    if parameters:
        ref["parameters"] = parameters
    return ref


def file_ref(name, folder):
    return dataset_ref(name, folder_path=expr(folder), file_name=FILE)


def activity(name, kind, props, *, after=None, retry=None, **extra):
    item = {"name": name, "type": kind, "typeProperties": props, **extra}
    if after:
        item["dependsOn"] = [{"activity": after, "dependencyConditions": ["Succeeded"]}]
    if retry is not None:
        item["policy"] = {"timeout": "0.01:00:00", "retry": retry, "retryIntervalInSeconds": 30}
    return item


def execute(name, pipeline, parameters, after=None):
    return activity(
        name,
        "ExecutePipeline",
        {
            "pipeline": {"referenceName": pipeline, "type": "PipelineReference"},
            "parameters": parameters,
            "waitOnCompletion": True,
        },
        after=after,
    )


def condition(name, predicate, children, *, after):
    return activity(name, "IfCondition", {"expression": expr(predicate), "ifTrueActivities": children}, after=after)


def fail(name, message):
    return activity(name, "Fail", {"errorCode": "RetailCdcContract", "message": message})


def folder(slice_expr, *, staging=False):
    prefix = "_staging/retail_db/" if staging else "retail_db/"
    table = select_ci({ci: ci.removeprefix("dbo_") for ci in TABLES})
    return f"concat('{prefix}', {table}, '/', {slice_expr})"


def slice_path(mode):
    if mode == "snapshot":
        return f"concat('snapshot=', {LOOKUP}.to_lsn)"
    return f"concat('window=', {LOOKUP}.from_lsn, '_', {LOOKUP}.to_lsn)"


def source_query(mode):
    columns = select_ci(COLUMNS)
    if mode == "snapshot":
        zero = literal("', '0x00000000000000000000' AS _seqval, 0 AS _op, " + SNAPSHOT_CDC + ", ")
        tables = select_ci({ci: "dbo.[" + ci.removeprefix("dbo_") + "]" for ci in TABLES})
        start = literal("SELECT _lsn = '")
        return f"concat({start}, {LOOKUP}.to_lsn, {zero}, {columns}, ' FROM ', {tables})"
    functions = select_ci({ci: f"cdc.[fn_cdc_get_all_changes_{ci}]" for ci in TABLES})
    projection = literal(
        "SELECT CONVERT(varchar(22), [__$start_lsn], 1) AS _lsn, "
        "CONVERT(varchar(22), [__$seqval], 1) AS _seqval, [__$operation] AS _op, " + RAW_CDC + ", "
    )
    left = literal("(CONVERT(binary(10), '")
    middle = literal("', 1), CONVERT(binary(10), '")
    right = literal("', 1), N'all')")
    return (
        f"concat({projection}, {columns}, ' FROM ', {functions}, "
        f"{left}, {LOOKUP}.from_lsn, {middle}, {LOOKUP}.to_lsn, {right})"
    )


def watermark(mode, after=None):
    params = {
        "capture_instance": proc_param(expr(CI)),
        "to_lsn": proc_param(expr(f"{LOOKUP}.to_lsn")),
        "adf_run_id": proc_param(expr("pipeline().parameters.parent_run_id")),
        "mode": proc_param(mode),
        "rows": proc_param(0 if mode == "empty" else expr(f"activity('copy_{mode}').output.rowsCopied"), "Int64"),
    }
    if mode == "window":
        params["from_lsn"] = proc_param(expr(f"{LOOKUP}.from_lsn"))
    return activity(
        f"set_watermark_{mode}",
        "SqlServerStoredProcedure",
        {"storedProcedureName": "etl.usp_set_cdc_watermark", "storedProcedureParameters": params},
        linkedServiceName=link("ls_sql_retail"),
        after=after,
        retry=2,
    )


def case_activities(mode):
    if mode == "empty":
        return [watermark(mode)]
    copy_name = f"copy_{mode}"
    publish = f"publish_{mode}"
    slice_expr = slice_path(mode)
    return [
        activity(
            copy_name,
            "Copy",
            {
                "source": {
                    "type": "AzureSqlSource",
                    "sqlReaderQuery": expr(source_query(mode)),
                    "partitionOption": "None",
                },
                "sink": {
                    "type": "ParquetSink",
                    "storeSettings": {"type": "AzureBlobFSWriteSettings"},
                    "formatSettings": {"type": "ParquetWriteSettings"},
                },
                "enableStaging": False,
                "parallelCopies": 1,
            },
            inputs=[dataset_ref("ds_retail_cdc_sql")],
            outputs=[file_ref("ds_retail_cdc_parquet", folder(slice_expr, staging=True))],
            retry=2,
        ),
        execute(
            publish,
            "pl_cdc_publish_slice",
            {
                "ci": expr(CI),
                "slice": expr(slice_expr),
                "rows_read": expr(f"string(activity('{copy_name}').output.rowsRead)"),
                "rows_copied": expr(f"string(activity('{copy_name}').output.rowsCopied)"),
            },
            after=copy_name,
        ),
        watermark(mode, after=publish),
    ]


def one_table():
    return [
        condition(
            "reject_missing_run_id",
            "empty(trim(pipeline().parameters.parent_run_id))",
            [fail("missing_run_id", "Cần ID lượt ingest để ghi log và chạy lại cùng cửa sổ.")],
            after=None,
        ),
        activity(
            "lk_window",
            "Lookup",
            {
                "source": {
                    "type": "AzureSqlSource",
                    "sqlReaderStoredProcedureName": "etl.usp_get_cdc_window",
                    "storedProcedureParameters": {"capture_instance": proc_param(expr(CI))},
                    "partitionOption": "None",
                },
                "dataset": dataset_ref("ds_retail_cdc_sql"),
                "firstRowOnly": True,
            },
            after="reject_missing_run_id",
            retry=2,
        ),
        activity(
            "switch_window_mode",
            "Switch",
            {
                "on": expr(f"{LOOKUP}.mode"),
                "cases": [{"value": m, "activities": case_activities(m)} for m in MODES],
                "defaultActivities": [fail("invalid_window_mode", "Lookup trả mode khác empty/snapshot/window.")],
            },
            after="lk_window",
        ),
    ]


def metadata(name, dataset, after):
    return activity(
        name,
        "GetMetadata",
        {"dataset": dataset, "fieldList": ["exists"], "storeSettings": {"type": "AzureBlobFSReadSettings"}},
        after=after,
        retry=2,
    )


def publish_slice():
    # Chỉ chấp nhận một khóa bảng và một phân đoạn đường dẫn. Không nhận đường dẫn tùy ý để xóa.
    ci_valid = f"or(equals({CI}, 'dbo_product_prices'), equals({CI}, 'dbo_promotions'))"
    segment = f"replace({SLICE}, '=', '')"
    slice_valid = (
        f"and(equals(uriComponent({segment}), {segment}), "
        f"or(and(startsWith({SLICE}, 'snapshot=0x'), equals(length({SLICE}), 31)), "
        f"and(startsWith({SLICE}, 'window=0x'), equals(length({SLICE}), 52))))"
    )
    counts_valid = f"and(greaterOrEquals(int({READ}), 0), equals(int({READ}), int({COPIED})))"
    positive = f"greater(int({COPIED}), 0)"
    stage_path = folder(SLICE, staging=True)
    staged = dataset_ref(
        "ds_instacart_adls_binary", file_system="landing", folder_path=expr(stage_path), file_name=FILE
    )
    landing = dataset_ref(
        "ds_instacart_adls_binary", file_system="landing", folder_path=expr(folder(SLICE)), file_name=FILE
    )
    stage_folder = dataset_ref(
        "ds_instacart_adls_binary", file_system="landing", folder_path=expr(stage_path), file_name=""
    )
    stage_exists = "activity('get_staged_file').output.exists"
    return [
        condition(
            "reject_invalid_slice",
            f"not(and({ci_valid}, and({slice_valid}, {counts_valid})))",
            [fail("bad_publish_input", "Sai bảng, phân đoạn LSN hoặc số dòng Copy không khớp.")],
            after=None,
        ),
        metadata("get_staged_file", staged, "reject_invalid_slice"),
        condition(
            "publish_if_staged",
            f"and({positive}, {stage_exists})",
            [
                activity(
                    "publish_parquet",
                    "Copy",
                    {
                        "source": {"type": "BinarySource", "storeSettings": {"type": "AzureBlobFSReadSettings"}},
                        "sink": {
                            "type": "BinarySink",
                            "storeSettings": {"type": "AzureBlobFSWriteSettings", "copyBehavior": "PreserveHierarchy"},
                        },
                        "enableStaging": False,
                        "validateDataConsistency": True,
                    },
                    inputs=[staged],
                    outputs=[landing],
                    retry=2,
                )
            ],
            after="get_staged_file",
        ),
        metadata("get_published_file", landing, "publish_if_staged"),
        condition(
            "require_published_file",
            f"and({positive}, not(activity('get_published_file').output.exists))",
            [fail("missing_published_file", "Copy có dòng nhưng không tìm thấy Parquet ở landing.")],
            after="get_published_file",
        ),
        metadata("get_staged_slice", stage_folder, "require_published_file"),
        condition(
            "clean_staged_slice",
            "activity('get_staged_slice').output.exists",
            [
                activity(
                    "delete_staged_parquet",
                    "Delete",
                    {"dataset": stage_folder, "storeSettings": {"type": "AzureBlobFSReadSettings", "recursive": True}},
                    retry=0,
                )
            ],
            after="get_staged_slice",
        ),
    ]


def pipeline_definitions():
    parent = activity(
        "for_each_capture_instance",
        "ForEach",
        {
            "items": expr(f"json({literal(json.dumps(TABLES))})"),
            "isSequential": True,
            "activities": [
                execute(
                    "ingest_one_table",
                    "pl_cdc_one_table",
                    {"ci": expr("item()"), "parent_run_id": expr("pipeline().RunId")},
                )
            ],
        },
    )
    return {
        "pl_ingest_retail_db": ([parent], {}),
        "pl_cdc_one_table": (one_table(), {"ci": "", "parent_run_id": ""}),
        "pl_cdc_publish_slice": (publish_slice(), {"ci": "", "slice": "", "rows_read": "-1", "rows_copied": "-1"}),
    }


def dataset_definitions():
    return {
        "ds_retail_cdc_sql": {"type": "AzureSqlTable", "linkedServiceName": link("ls_sql_retail")},
        "ds_retail_cdc_parquet": {
            "type": "Parquet",
            "linkedServiceName": link("ls_adls"),
            "parameters": {name: {"type": "String"} for name in ("folder_path", "file_name")},
            "typeProperties": {
                "location": {
                    "type": "AzureBlobFSLocation",
                    "fileSystem": "landing",
                    "folderPath": expr("dataset().folder_path"),
                    "fileName": expr("dataset().file_name"),
                },
                "compressionCodec": "snappy",
            },
        },
    }


def definition(name, activities, params):
    return {
        "name": name,
        "properties": {
            "activities": activities,
            "concurrency": 1,
            "parameters": {key: {"type": "String", "defaultValue": value} for key, value in params.items()},
        },
    }


def artifacts():
    files = {}
    for name, (activities, params) in pipeline_definitions().items():
        files[f"pipelines/{name}.json"] = activities
        files[f"pipeline_settings/{name}.json"] = {"concurrency": 1, "parameters": params}
    for name, props in dataset_definitions().items():
        files[f"datasets/{name}.json"] = {"name": name, "properties": props}
    return files


def sdk_check():
    from azure.mgmt.datafactory.models import DatasetResource, PipelineResource

    payloads = [(PipelineResource, definition(n, a, p)) for n, (a, p) in pipeline_definitions().items()]
    payloads.extend((DatasetResource, {"name": n, "properties": p}) for n, p in dataset_definitions().items())
    for model, data in payloads:
        # name là trường từ ARM trả về, SDK loại khỏi payload gửi đi.
        round_trip = model.deserialize(data).serialize()
        if round_trip != {"properties": data["properties"]}:
            raise ValueError(f"Azure SDK đổi hoặc làm mất trường trong {data['name']}")
    print(f"Azure SDK round-trip đạt {len(payloads)} resource.")


def dump(data):
    return json.dumps(data, ensure_ascii=False, indent=2) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--sdk-check", action="store_true")
    parser.add_argument("--definition", choices=pipeline_definitions())
    args = parser.parse_args()
    if args.definition:
        print(dump(definition(args.definition, *pipeline_definitions()[args.definition])), end="")
        return
    for path, content in artifacts().items():
        path = ROOT / path
        if args.check:
            if not path.is_file() or path.read_text() != dump(content):
                parser.error(f"JSON chưa đồng bộ: {path.name}")
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(dump(content))
    if args.sdk_check:
        sdk_check()


if __name__ == "__main__":
    main()
