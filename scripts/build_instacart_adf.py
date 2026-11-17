#!/usr/bin/env python3
"""Dựng JSON ADF từ hợp đồng nguồn; không kết nối/publish Azure hay sửa Terraform."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def expr(value):
    return {"value": value, "type": "Expression"}


def ref(name, parameters):
    return {"referenceName": name, "type": "DatasetReference", "parameters": parameters}


def activity(name, kind, properties, *, after=None, description=None, retry=5, **extra):
    result = {
        "name": name,
        "type": kind,
        "dependsOn": [] if after is None else [{"activity": after, "dependencyConditions": ["Succeeded"]}],
    }
    if description:
        result["description"] = description
    if kind in ("Copy", "Delete", "GetMetadata"):
        result["policy"] = {
            "timeout": "0.02:00:00",
            "retry": retry,
            "retryIntervalInSeconds": 60,
            "secureOutput": False,
            "secureInput": False,
        }
    return result | extra | {"typeProperties": properties}


def binary_copy(name, source, sink, after=None):
    return activity(
        name,
        "Copy",
        {
            "source": {
                "type": "BinarySource",
                "storeSettings": {"type": "AzureBlobFSReadSettings", "recursive": False},
            },
            "sink": {
                "type": "BinarySink",
                "storeSettings": {"type": "AzureBlobFSWriteSettings", "copyBehavior": "PreserveHierarchy"},
            },
            "enableStaging": False,
            "validateDataConsistency": True,
            "skipErrorFile": {"dataInconsistency": False},
        },
        after=after,
        inputs=[source],
        outputs=[sink],
    )


def render(root=ROOT):
    source = json.loads((root / "adf/instacart_sources.json").read_text())
    tables = source["tables"]
    mappings = {
        table: [
            {"source": {"name": column, "type": "String"}, "sink": {"name": column, "type": "String"}}
            for column in info["columns"]
        ]
        for table, info in tables.items()
    }
    translators = {
        table: {"type": "TabularTranslator", "mappings": columns, "typeConversion": False}
        for table, columns in mappings.items()
    }
    # Provider azurerm dùng map(string) cho pipeline.parameters, nên JSON đi qua String + json().
    settings = {
        "pl_ingest_instacart": {
            "concurrency": 1,
            "parameters": {
                "tables_json": json.dumps(list(tables)),
                "expected_rows": json.dumps({table: info["rows"] for table, info in tables.items()}),
                "translators_json": json.dumps(translators, separators=(",", ":")),
            },
        },
        "pl_ingest_instacart_table": {
            "concurrency": 1,
            "parameters": {"table_name": "", "expected_rows": "0", "translator_json": "{}"},
        },
    }

    def adls(folder, filename, dataset="ds_instacart_adls_binary"):
        return ref(dataset, {"file_system": "landing", "folder_path": folder, "file_name": filename})

    release = source["release"]
    done_folder = expr(f"@concat('instacart/', item(), '/release={release}')")
    parent = [
        activity(
            "for_each_table",
            "ForEach",
            {
                "items": expr("@json(pipeline().parameters.tables_json)"),
                "isSequential": True,
                "activities": [
                    activity(
                        "get_success",
                        "GetMetadata",
                        {
                            "dataset": adls(done_folder, "_SUCCESS"),
                            "fieldList": ["exists"],
                            "storeSettings": {"type": "AzureBlobFSReadSettings", "recursive": False},
                        },
                    ),
                    activity(
                        "if_not_published",
                        "IfCondition",
                        {
                            "expression": expr("@not(activity('get_success').output.exists)"),
                            "ifTrueActivities": [
                                activity(
                                    "ingest_one_table",
                                    "ExecutePipeline",
                                    {
                                        "pipeline": {
                                            "referenceName": "pl_ingest_instacart_table",
                                            "type": "PipelineReference",
                                        },
                                        "waitOnCompletion": True,
                                        "parameters": {
                                            "table_name": expr("@item()"),
                                            "expected_rows": expr(
                                                "@string(json(pipeline().parameters.expected_rows)[item()])"
                                            ),
                                            "translator_json": expr(
                                                "@string(json(pipeline().parameters.translators_json)[item()])"
                                            ),
                                        },
                                    },
                                )
                            ],
                            "ifFalseActivities": [],
                        },
                        after="get_success",
                    ),
                ],
            },
            description="Lần hai chỉ kiểm marker và bỏ qua; không dùng biến dùng chung trong vòng lặp.",
        )
    ]
    filename = expr("@concat(pipeline().parameters.table_name, '.csv')")
    stage_folder = expr(f"@concat('_staging/instacart/', pipeline().parameters.table_name, '/release={release}')")
    final_folder = expr(f"@concat('instacart/', pipeline().parameters.table_name, '/release={release}')")
    staged = adls(stage_folder, filename)
    published = adls(final_folder, filename)
    marker = adls(final_folder, "_SUCCESS")
    child = [
        activity(
            "get_marker_template",
            "GetMetadata",
            {
                "dataset": adls("ops", "_SUCCESS"),
                "fieldList": ["size"],
                "storeSettings": {"type": "AzureBlobFSReadSettings", "recursive": False},
            },
            retry=0,
        ),
        activity(
            "if_empty_template",
            "IfCondition",
            {
                "expression": expr("@equals(activity('get_marker_template').output.size, 0)"),
                "ifTrueActivities": [],
                "ifFalseActivities": [
                    activity(
                        "fail_marker_template",
                        "Fail",
                        {
                            "message": "Template landing/ops/_SUCCESS phải là file 0 byte.",
                            "errorCode": "INSTACART_MARKER_TEMPLATE",
                        },
                    )
                ],
            },
            after="get_marker_template",
        ),
        activity(
            "copy_csv_to_staging",
            "Copy",
            {
                "source": {
                    "type": "DelimitedTextSource",
                    "storeSettings": {
                        "type": "HttpReadSettings",
                        "requestMethod": "GET",
                        "httpRequestTimeout": "00:05:00",
                    },
                    "formatSettings": {"type": "DelimitedTextReadSettings"},
                },
                "sink": {
                    "type": "DelimitedTextSink",
                    "storeSettings": {"type": "AzureBlobFSWriteSettings"},
                    "formatSettings": {"type": "DelimitedTextWriteSettings", "quoteAllText": True},
                },
                "translator": expr("@json(pipeline().parameters.translator_json)"),
                "enableStaging": False,
                "enableSkipIncompatibleRow": False,
            },
            after="if_empty_template",
            inputs=[ref("ds_instacart_http_csv", {"file_name": filename})],
            outputs=[adls(stage_folder, filename, "ds_instacart_adls_csv")],
        ),
        activity(
            "if_rows_match",
            "IfCondition",
            {
                "expression": expr(
                    "@and(equals(activity('copy_csv_to_staging').output.rowsCopied, "
                    "int(pipeline().parameters.expected_rows)), "
                    "equals(activity('copy_csv_to_staging').output.rowsRead, "
                    "int(pipeline().parameters.expected_rows)))"
                ),
                "ifTrueActivities": [
                    binary_copy("publish_csv", staged, published),
                    activity(
                        "delete_staging",
                        "Delete",
                        {
                            "dataset": adls(stage_folder, ""),
                            "enableLogging": False,
                            "storeSettings": {"type": "AzureBlobFSReadSettings", "recursive": True},
                        },
                        after="publish_csv",
                    ),
                    binary_copy("write_success", adls("ops", "_SUCCESS"), marker, after="delete_staging"),
                ],
                "ifFalseActivities": [
                    activity(
                        "fail_rows",
                        "Fail",
                        {
                            "message": expr(
                                "@concat('Số dòng không khớp cho ', pipeline().parameters.table_name, "
                                "'; cần ', pipeline().parameters.expected_rows)"
                            ),
                            "errorCode": "INSTACART_ROW_COUNT",
                        },
                    )
                ],
            },
            after="copy_csv_to_staging",
            description="Thiếu rowsCopied/rowsRead làm biểu thức lỗi: không được coi như đã đối soát đạt.",
        ),
        activity(
            "get_written_success",
            "GetMetadata",
            {
                "dataset": marker,
                "fieldList": ["exists"],
                "storeSettings": {"type": "AzureBlobFSReadSettings", "recursive": False},
            },
            after="if_rows_match",
        ),
        activity(
            "if_marker_exists",
            "IfCondition",
            {
                "expression": expr("@activity('get_written_success').output.exists"),
                "ifTrueActivities": [],
                "ifFalseActivities": [
                    activity(
                        "fail_missing_marker",
                        "Fail",
                        {"message": "Chưa có _SUCCESS sau khi Copy marker.", "errorCode": "INSTACART_MISSING_MARKER"},
                    )
                ],
            },
            after="get_written_success",
        ),
    ]
    datasets = {}
    csv_settings = {
        "columnDelimiter": ",",
        "rowDelimiter": "\n",
        "quoteChar": '"',
        "escapeChar": '"',
        "firstRowAsHeader": True,
        "nullValue": "",
        "encodingName": "UTF-8 without BOM",
    }
    for name, kind, linked in [
        ("ds_instacart_http_csv", "DelimitedText", "ls_zenodo"),
        ("ds_instacart_adls_csv", "DelimitedText", "ls_adls"),
        ("ds_instacart_adls_binary", "Binary", "ls_adls"),
    ]:
        http = linked == "ls_zenodo"
        location = (
            {
                "type": "HttpServerLocation",
                "relativeUrl": expr(
                    f"@concat('api/records/{source['record']}/files/', dataset().file_name, '/content')"
                ),
            }
            if http
            else {
                "type": "AzureBlobFSLocation",
                "fileSystem": expr("@dataset().file_system"),
                "folderPath": expr("@dataset().folder_path"),
                "fileName": expr("@dataset().file_name"),
            }
        )
        datasets[name] = {
            "name": name,
            "properties": {
                "linkedServiceName": {"referenceName": linked, "type": "LinkedServiceReference"},
                "parameters": {
                    key: {"type": "String", "defaultValue": "landing" if key == "file_system" else ""}
                    for key in (["file_name"] if http else ["file_system", "folder_path", "file_name"])
                },
                "type": kind,
                "typeProperties": {"location": location}
                | (
                    {key: value for key, value in csv_settings.items() if not (http and key == "rowDelimiter")}
                    if kind == "DelimitedText"
                    else {}
                ),
                "schema": [],
            },
        }
    artifacts = {
        f"adf/pipelines/{name}.json": activities
        for name, activities in [("pl_ingest_instacart", parent), ("pl_ingest_instacart_table", child)]
    }
    artifacts |= {f"adf/pipeline_settings/{name}.json": config for name, config in settings.items()}
    artifacts |= {f"adf/datasets/{name}.json": config for name, config in datasets.items()}
    return artifacts


def full_definition(name, artifacts):
    settings = artifacts[f"adf/pipeline_settings/{name}.json"]
    return {
        "name": name,
        "properties": {
            "concurrency": settings["concurrency"],
            "parameters": {
                key: {"type": "String", "defaultValue": value} for key, value in settings["parameters"].items()
            },
            "activities": artifacts[f"adf/pipelines/{name}.json"],
        },
    }


def validate_sdk(artifacts):
    """Kiểm model chính thức, không tạo client Azure hoặc gửi request."""
    from azure.mgmt.datafactory.models import DatasetResource, PipelineResource

    for name in ("pl_ingest_instacart", "pl_ingest_instacart_table"):
        source = full_definition(name, artifacts)
        actual = PipelineResource.deserialize(source).serialize(keep_readonly=True)
        if actual["properties"] != source["properties"]:
            raise ValueError(f"SDK không giữ nguyên thuộc tính pipeline {name}")
    for path, source in artifacts.items():
        if "/datasets/" in path:
            actual = DatasetResource.deserialize(source).serialize(keep_readonly=True)
            if actual["properties"] != source["properties"]:
                raise ValueError(f"SDK không giữ nguyên thuộc tính dataset {path}")
    print("SDK round-trip đạt: 2 pipeline, 3 dataset (không gọi Azure)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Chỉ kiểm JSON đã commit khớp nguồn dựng")
    parser.add_argument("--sdk-check", action="store_true", help="Kiểm chỉ đọc bằng azure-mgmt-datafactory==9.2.0")
    parser.add_argument(
        "--definition",
        choices=("pl_ingest_instacart", "pl_ingest_instacart_table"),
        help="In định nghĩa đầy đủ để import/debug, không publish",
    )
    args = parser.parse_args()
    artifacts = render()
    if args.definition:
        print(json.dumps(full_definition(args.definition, artifacts), ensure_ascii=False, indent=2))
        return
    for path, data in artifacts.items():
        target = ROOT / path
        content = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
        if args.check or args.sdk_check:
            if not target.exists() or target.read_text() != content:
                raise SystemExit(f"JSON chưa đồng bộ: {path}; chạy python3 scripts/build_instacart_adf.py")
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
    if args.sdk_check:
        validate_sdk(artifacts)
    print(f"{'Đã kiểm' if args.check or args.sdk_check else 'Đã dựng'} {len(artifacts)} JSON ADF")


if __name__ == "__main__":
    main()
