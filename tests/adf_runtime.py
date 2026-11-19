"""Mô phỏng tập hoạt động/biểu thức dùng ở JSON này trên file local; không phải engine ADF."""

from __future__ import annotations

import ast
import csv
import json
import operator
import re
import shutil
from pathlib import Path
from urllib.parse import quote


class PipelineFailure(RuntimeError):
    pass


class LocalADF:
    def __init__(self, root: Path, source: Path, repository: Path):
        self.root, self.source = root, source
        self.pipelines = {p.stem: json.loads(p.read_text()) for p in (repository / "adf/pipelines").glob("*.json")}
        self.settings = {
            p.stem: json.loads(p.read_text()) for p in (repository / "adf/pipeline_settings").glob("*.json")
        }
        self.datasets = {
            p.stem: json.loads(p.read_text())["properties"] for p in (repository / "adf/datasets").glob("*.json")
        }
        self.trace = []
        self.fail_at = None
        self.copy_output = None
        self.drop_marker = False
        self.run_id = "local-adf-run"
        template = root / "landing/ops/_SUCCESS"
        template.parent.mkdir(parents=True)
        template.write_bytes(b"")

    def value(self, value, context):
        if isinstance(value, dict):
            if value.get("type") == "Expression":
                expression = re.sub(
                    r"'(?:[^']|'')*'|\b(and|or|not)\(",
                    lambda token: f"{token[1]}_(" if token[1] else repr(token[0][1:-1].replace("''", "'")),
                    value["value"][1:],
                )
                functions = {
                    "pipeline": lambda: {"parameters": context["parameters"], "RunId": self.run_id},
                    "dataset": lambda: context["dataset"],
                    "activity": lambda name: {"output": context["outputs"][name]},
                    "item": lambda: context["item"],
                    "json": json.loads,
                    "string": lambda x: json.dumps(x) if isinstance(x, (list, dict)) else str(x),
                    "int": int,
                    "concat": lambda *x: "".join(map(str, x)),
                    "equals": lambda a, b: a == b,
                    "and_": lambda a, b: a and b,
                    "not_": operator.not_,
                    "or_": lambda *values: any(values),
                    "greater": operator.gt,
                    "greaterOrEquals": operator.ge,
                    "empty": operator.not_,
                    "trim": str.strip,
                    "length": len,
                    "replace": str.replace,
                    "startsWith": str.startswith,
                    "uriComponent": lambda text: quote(text, safe=""),
                }

                def evaluate(node):
                    if isinstance(node, ast.Constant):
                        return node.value
                    if isinstance(node, ast.Attribute):
                        return evaluate(node.value)[node.attr]
                    if isinstance(node, ast.Subscript):
                        return evaluate(node.value)[evaluate(node.slice)]
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                        return functions[node.func.id](*[evaluate(arg) for arg in node.args])
                    raise ValueError(f"Biểu thức chưa được mô phỏng: {ast.dump(node)}")

                return evaluate(ast.parse(expression, mode="eval").body)
            return {key: self.value(item, context) for key, item in value.items()}
        if isinstance(value, list):
            return [self.value(item, context) for item in value]
        return value

    def dataset(self, reference, context):
        definition = self.datasets[reference["referenceName"]]
        parameters = {key: info.get("defaultValue") for key, info in definition.get("parameters", {}).items()}
        parameters |= self.value(reference.get("parameters", {}), context)
        location = self.value(definition["typeProperties"]["location"], context | {"dataset": parameters})
        if location["type"] == "HttpServerLocation":
            path = self.source / parameters["file_name"]
        else:
            path = self.root / location["fileSystem"] / location["folderPath"] / location["fileName"]
            path.resolve().relative_to(self.root.resolve())
        return path, definition

    def run(self, pipeline="pl_ingest_instacart", **parameters):
        defaults = self.settings[pipeline]["parameters"]
        context = {"parameters": defaults | parameters, "outputs": {}, "item": None}
        self.execute(self.pipelines[pipeline], context)

    def execute(self, activities, context):
        pending, completed = list(activities), set()
        while pending:
            # Bộ mô phỏng chỉ hỗ trợ dependency Succeeded; từ chối kiểu khác thay vì bỏ qua.
            if any(d["dependencyConditions"] != ["Succeeded"] for a in pending for d in a.get("dependsOn", [])):
                raise ValueError("Chưa mô phỏng dependencyConditions ngoài Succeeded")
            available = [a for a in pending if all(d["activity"] in completed for d in a.get("dependsOn", []))]
            if not available:
                raise ValueError("Chu trình hoặc dependency sai scope")
            for a in available:
                name, kind, props = a["name"], a["type"], a["typeProperties"]
                self.trace.append((name, context["item"] or context["parameters"].get("table_name")))
                if self.fail_at == (name, "before"):
                    self.fail_at = None
                    raise PipelineFailure(name)
                if kind == "ForEach":
                    for item in self.value(props["items"], context):
                        self.execute(props["activities"], context | {"item": item})
                elif kind == "IfCondition":
                    branch = "ifTrueActivities" if self.value(props["expression"], context) else "ifFalseActivities"
                    self.execute(props.get(branch, []), context)
                elif kind == "Switch":
                    value = self.value(props["on"], context)
                    cases = {case["value"]: case["activities"] for case in props["cases"]}
                    self.execute(cases.get(value, props["defaultActivities"]), context)
                elif kind == "ExecutePipeline":
                    assert props["waitOnCompletion"] is True
                    self.run(props["pipeline"]["referenceName"], **self.value(props["parameters"], context))
                elif kind == "Fail":
                    raise PipelineFailure(props["errorCode"])
                elif kind == "GetMetadata":
                    path, _ = self.dataset(props["dataset"], context)
                    output = {"exists": path.exists()}
                    if "size" in props["fieldList"]:
                        output["size"] = path.stat().st_size
                    context["outputs"][name] = output
                elif kind in ("Lookup", "SqlServerStoredProcedure") or (
                    kind == "Copy" and props["source"]["type"] == "AzureSqlSource"
                ):
                    context["outputs"][name] = self.sql_activity(a, context)
                elif kind == "Copy":
                    src, source_definition = self.dataset(a["inputs"][0], context)
                    dst, sink_definition = self.dataset(a["outputs"][0], context)
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    if props["source"]["type"] == "DelimitedTextSource":
                        source_format = source_definition["typeProperties"]
                        sink_format = sink_definition["typeProperties"]
                        with src.open(newline="") as stream:
                            rows = list(csv.DictReader(stream, quotechar=source_format["quoteChar"]))
                        translator = self.value(props["translator"], context)
                        mappings = translator["mappings"]
                        with dst.open("w", newline="") as stream:
                            writer = csv.writer(stream, quotechar=sink_format["quoteChar"], quoting=csv.QUOTE_ALL)
                            writer.writerow([m["sink"]["name"] for m in mappings])
                            writer.writerows([[row[m["source"]["name"]] for m in mappings] for row in rows])
                        context["outputs"][name] = (
                            self.copy_output
                            if self.copy_output is not None
                            else {"rowsRead": len(rows), "rowsCopied": len(rows)}
                        )
                    elif not (self.drop_marker and name == "write_success"):
                        shutil.copyfile(src, dst)
                    if self.fail_at == (name, "partial"):
                        self.fail_at = None
                        dst.write_bytes(dst.read_bytes()[:8])
                        raise PipelineFailure(name)
                elif kind == "Delete":
                    path, _ = self.dataset(props["dataset"], context)
                    assert props["storeSettings"]["recursive"] is True
                    shutil.rmtree(path)
                else:
                    raise ValueError(f"Hoạt động chưa được mô phỏng: {kind}")
                if self.fail_at == (name, "after"):
                    self.fail_at = None
                    raise PipelineFailure(name)
                completed.add(name)
                pending.remove(a)

    def sql_activity(self, activity, context):
        raise ValueError(f"Chưa có fixture SQL cho {activity['name']}")
