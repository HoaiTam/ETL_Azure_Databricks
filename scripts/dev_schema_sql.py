"""In DDL tạo bốn schema dev; người dùng chạy SQL trên warehouse, script không kết nối cloud."""

from __future__ import annotations

import argparse
import re


def dev_schema_sql(catalog: str, schema_prefix: str) -> str:
    """Bắt buộc tiền tố riêng và quote identifier để không tạo schema prod hay chèn SQL."""
    if not catalog or not catalog.strip() or any(ord(char) < 32 for char in catalog):
        raise ValueError("Catalog phải khác rỗng và không chứa ký tự điều khiển")
    if not re.fullmatch(r"[\w-]+_", schema_prefix) or not schema_prefix.rstrip("_"):
        raise ValueError("Tiền tố dev phải có tên người dùng, kết thúc bằng _ và chỉ chứa chữ, số, _, -")
    quoted_catalog = "`" + catalog.replace("`", "``") + "`"
    return "\n".join(
        f"CREATE SCHEMA IF NOT EXISTS {quoted_catalog}.`{schema_prefix}{layer}`;"
        for layer in ("bronze", "silver", "gold", "ops")
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="instacart")
    parser.add_argument("--schema-prefix", required=True, help="Đúng schema_prefix đã resolve ở bundle dev")
    args = parser.parse_args()
    try:
        print(dev_schema_sql(args.catalog, args.schema_prefix))
    except ValueError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
