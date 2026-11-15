"""Task `ingest_bronze`: Volume raw → bronze.* bằng Auto Loader (availableNow).

Logic chi tiết theo tài liệu DE; file này mới là khung.
"""

from __future__ import annotations

import argparse


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="ingest_bronze")
    parser.add_argument("--catalog", default="instacart")
    args = parser.parse_args(argv)
    print(f"[ingest_bronze] catalog={args.catalog}: TODO")


if __name__ == "__main__":
    main()
