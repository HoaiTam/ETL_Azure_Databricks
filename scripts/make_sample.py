"""Lấy mẫu Instacart theo khách hàng, giữ nguyên lịch sử và khóa gốc."""

from __future__ import annotations

import argparse
import csv
import json
import random
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from decimal import ROUND_CEILING, Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

REQUIRED_COLUMNS = {
    "orders.csv": (
        "order_id",
        "user_id",
        "eval_set",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "days_since_prior_order",
    ),
    "order_products__prior.csv": ("order_id", "product_id", "add_to_cart_order", "reordered"),
    "order_products__train.csv": ("order_id", "product_id", "add_to_cart_order", "reordered"),
    "products.csv": ("product_id", "product_name", "aisle_id", "department_id"),
    "aisles.csv": ("aisle_id", "aisle"),
    "departments.csv": ("department_id", "department"),
}


@contextmanager
def csv_rows(path: Path) -> Iterator[tuple[list[str], Iterator[list[str]]]]:
    """Đọc tuần tự, hỗ trợ CSV có BOM và tên sản phẩm chứa dấu phẩy."""
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader, [])
        missing = set(REQUIRED_COLUMNS[path.name]) - set(header)
        if missing:
            raise ValueError(f"{path.name}: thiếu cột {', '.join(sorted(missing))}")
        yield header, reader


def copy_items(source: Path, target: Path, order_ids: set[int]) -> int:
    """Lọc từng dòng món hàng bằng khóa đơn, không nạp cả file vào RAM."""
    count = 0
    with csv_rows(source) as (header, rows), target.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        index = header.index("order_id")
        for row in rows:
            if int(row[index]) in order_ids:
                writer.writerow(row)
                count += 1
    return count


def make_sample(input_dir: Path, output_dir: Path, fraction: Decimal = Decimal("0.01"), seed: int = 42) -> dict:
    """Chọn ceil(tỷ lệ × số khách), ghi đủ sáu CSV và manifest sau khi thành công."""
    if not fraction.is_finite() or not Decimal(0) < fraction <= Decimal(1):
        raise ValueError("fraction phải lớn hơn 0 và không quá 1")
    input_dir, output_dir = input_dir.resolve(), output_dir.resolve()
    if input_dir.is_relative_to(output_dir) or output_dir.is_relative_to(input_dir):
        raise ValueError("Thư mục nguồn và đích phải tách biệt, không lồng nhau")
    if output_dir.exists():
        raise FileExistsError(f"Đích đã tồn tại: {output_dir}; hãy chọn thư mục mới")
    for name in REQUIRED_COLUMNS:
        with csv_rows(input_dir / name):
            pass

    with csv_rows(input_dir / "orders.csv") as (header, rows):
        index = header.index("user_id")
        users = sorted({int(row[index]) for row in rows})
    if not users:
        raise ValueError("orders.csv không có khách hàng để lấy mẫu")
    size = int((fraction * len(users)).to_integral_value(rounding=ROUND_CEILING))
    selected = set(random.Random(seed).sample(users, size))

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    # Công bố cả thư mục sau cùng: lỗi đọc file không để lại mẫu nửa chừng.
    with TemporaryDirectory(prefix=".instacart-sample-", dir=output_dir.parent) as temporary:
        stage = Path(temporary)
        order_ids: dict[str, set[int]] = {"prior": set(), "train": set(), "test": set()}
        with (
            csv_rows(input_dir / "orders.csv") as (header, rows),
            (stage / "orders.csv").open("w", encoding="utf-8", newline="") as handle,
        ):
            writer = csv.writer(handle)
            writer.writerow(header)
            user_index, order_index, split_index = (header.index(key) for key in ("user_id", "order_id", "eval_set"))
            for row in rows:
                if int(row[user_index]) in selected:
                    split = row[split_index]
                    if split not in order_ids:
                        raise ValueError(f"orders.csv: eval_set không hợp lệ: {split}")
                    order_ids[split].add(int(row[order_index]))
                    writer.writerow(row)

        counts = {"orders.csv": sum(len(ids) for ids in order_ids.values())}
        for split in ("prior", "train"):
            name = f"order_products__{split}.csv"
            counts[name] = copy_items(input_dir / name, stage / name, order_ids[split])
        # Giữ toàn bộ danh mục để mọi FK còn hợp lệ và không đánh lại mã item.
        for name in ("products.csv", "aisles.csv", "departments.csv"):
            shutil.copyfile(input_dir / name, stage / name)
            with csv_rows(input_dir / name) as (_, rows):
                counts[name] = sum(1 for _ in rows)

        manifest = {
            "fraction": str(fraction.normalize()),
            "seed": seed,
            "source_users": len(users),
            "sample_users": size,
            "user_ids": sorted(selected),
            "orders_by_eval_set": {split: len(ids) for split, ids in order_ids.items()},
            "rows": counts,
        }
        (stage / "_manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        stage.rename(output_dir)
    return manifest


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True, help="Thư mục chứa sáu CSV Instacart đã giải nén")
    parser.add_argument("--output-dir", type=Path, default=Path("data/sample_1pct"), help="Thư mục mới để lưu mẫu")
    parser.add_argument("--fraction", default="0.01", help="Tỷ lệ khách hàng; mặc định 0.01 = 1%%")
    parser.add_argument("--seed", type=int, default=42, help="Seed cố định để tái lập tập khách hàng")
    args = parser.parse_args(argv)
    try:
        manifest = make_sample(args.input_dir, args.output_dir, Decimal(args.fraction), args.seed)
    except (OSError, ValueError, ArithmeticError, csv.Error) as error:
        parser.exit(1, f"Lỗi: {error}\n")
    summary = {key: value for key, value in manifest.items() if key != "user_ids"}
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
