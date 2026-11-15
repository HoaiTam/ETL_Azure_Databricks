"""Task `build_gold`: silver.* → gold.*, OPTIMIZE / liquid clustering ở cuối.

Logic chi tiết theo tài liệu DE; file này mới là khung.
"""

from __future__ import annotations

import argparse


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="build_gold")
    parser.add_argument("--catalog", default="instacart")
    args = parser.parse_args(argv)
    print(f"[build_gold] catalog={args.catalog}: TODO")


if __name__ == "__main__":
    main()
