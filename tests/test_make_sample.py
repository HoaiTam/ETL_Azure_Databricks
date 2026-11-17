from __future__ import annotations

import csv
import json
import shutil
from decimal import Decimal
from pathlib import Path

import pytest

from make_sample import REQUIRED_COLUMNS, main, make_sample

FIXTURE = Path(__file__).parent / "fixtures" / "instacart"


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def assert_complete_histories(source: Path, output: Path) -> None:
    """Đối chiếu từng dòng nguồn, gồm cả header test và mọi món của giỏ được chọn."""
    sampled_orders = read_rows(output / "orders.csv")
    users = {row["user_id"] for row in sampled_orders}
    expected_orders = [row for row in read_rows(source / "orders.csv") if row["user_id"] in users]
    assert sampled_orders == expected_orders
    order_ids = {row["order_id"] for row in expected_orders}
    for split in ("prior", "train"):
        name = f"order_products__{split}.csv"
        assert read_rows(output / name) == [row for row in read_rows(source / name) if row["order_id"] in order_ids]
    products = {row["product_id"] for row in read_rows(output / "products.csv")}
    for split in ("prior", "train"):
        assert all(row["product_id"] in products for row in read_rows(output / f"order_products__{split}.csv"))


def test_sample_keeps_all_splits_items_and_catalogs(tmp_path):
    output = tmp_path / "sample"
    manifest = make_sample(FIXTURE, output, Decimal("0.5"))

    assert manifest["sample_users"] == 3
    assert set(manifest["orders_by_eval_set"]) == {"prior", "train", "test"}
    assert all(count > 0 for count in manifest["orders_by_eval_set"].values())
    assert_complete_histories(FIXTURE, output)
    for name in ("products.csv", "aisles.csv", "departments.csv"):
        assert (output / name).read_bytes() == (FIXTURE / name).read_bytes()
    assert read_rows(output / "products.csv")[1]["product_name"] == "Tea, Green"
    assert any(row["days_since_prior_order"] == "" for row in read_rows(output / "orders.csv"))
    assert json.loads((output / "_manifest.json").read_text()) == manifest
    assert manifest["rows"] == {name: len(read_rows(output / name)) for name in REQUIRED_COLUMNS}


def test_default_one_percent_on_200_users_keeps_complete_histories(tmp_path):
    source = tmp_path / "source"
    shutil.copytree(FIXTURE, source)
    with (
        (source / "orders.csv").open("w", newline="") as orders,
        (source / "order_products__prior.csv").open("w", newline="") as prior,
        (source / "order_products__train.csv").open("w", newline="") as train,
    ):
        ow, pw, tw = csv.writer(orders), csv.writer(prior), csv.writer(train)
        ow.writerow(REQUIRED_COLUMNS["orders.csv"])
        pw.writerow(REQUIRED_COLUMNS["order_products__prior.csv"])
        tw.writerow(REQUIRED_COLUMNS["order_products__train.csv"])
        for user in range(1, 201):
            final_split = "train" if user % 2 else "test"
            for number, split in enumerate(("prior", "prior", final_split), 1):
                order = user * 10 + number
                ow.writerow([order, user, split, number, 0, 8, "" if number == 1 else "2.0"])
                if split != "test":
                    writer = pw if split == "prior" else tw
                    writer.writerows([[order, 1, 1, 0], [order, 2, 2, 0]])

    output = tmp_path / "sample"
    manifest = make_sample(source, output)
    assert manifest["source_users"] == 200
    assert manifest["sample_users"] == 2
    assert len(read_rows(output / "orders.csv")) == 6
    assert manifest["orders_by_eval_set"] == {"prior": 4, "train": 1, "test": 1}
    assert_complete_histories(source, output)


def test_sample_is_repeatable_and_independent_of_order_row_order(tmp_path):
    first, second, reordered = (tmp_path / name for name in ("first", "second", "reordered"))
    a = make_sample(FIXTURE, first, Decimal("0.5"), seed=123)
    b = make_sample(FIXTURE, second, Decimal("0.5"), seed=123)
    assert a == b
    for name in (*REQUIRED_COLUMNS, "_manifest.json"):
        assert (first / name).read_bytes() == (second / name).read_bytes()

    shutil.copytree(FIXTURE, reordered)
    rows = read_rows(reordered / "orders.csv")
    with (reordered / "orders.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REQUIRED_COLUMNS["orders.csv"])
        writer.writeheader()
        writer.writerows(reversed(rows))
    c = make_sample(reordered, tmp_path / "third", Decimal("0.5"), seed=123)
    assert c["user_ids"] == a["user_ids"]


def test_full_fraction_keeps_every_row_and_small_fraction_rounds_up(tmp_path):
    make_sample(FIXTURE, tmp_path / "full", Decimal("1"))
    for name in REQUIRED_COLUMNS:
        assert read_rows(tmp_path / "full" / name) == read_rows(FIXTURE / name)
    assert make_sample(FIXTURE, tmp_path / "small")["sample_users"] == 1


@pytest.mark.parametrize("fraction", ["0", "-0.01", "1.01", "NaN", "Infinity"])
def test_invalid_fraction_does_not_publish_output(tmp_path, fraction):
    output = tmp_path / "sample"
    with pytest.raises(ValueError, match="fraction"):
        make_sample(FIXTURE, output, Decimal(fraction))
    assert not output.exists()


def test_existing_output_and_nested_paths_are_rejected(tmp_path):
    source = tmp_path / "source"
    shutil.copytree(FIXTURE, source)
    with pytest.raises(ValueError, match="tách biệt"):
        make_sample(source, source / "sample")
    with pytest.raises(ValueError, match="tách biệt"):
        make_sample(source, source.parent)
    output = tmp_path / "sample"
    output.mkdir()
    marker = output / "keep.txt"
    marker.write_text("giữ nguyên")
    with pytest.raises(FileExistsError):
        make_sample(source, output)
    assert marker.read_text() == "giữ nguyên"


def test_failure_during_item_read_does_not_publish_partial_sample(tmp_path):
    source, output = tmp_path / "source", tmp_path / "sample"
    shutil.copytree(FIXTURE, source)
    with (source / "order_products__prior.csv").open("a") as handle:
        handle.write("invalid,1,1,0\n")
    with pytest.raises(ValueError):
        make_sample(source, output)
    assert not output.exists()
    assert not list(tmp_path.glob(".instacart-sample-*"))


def test_missing_source_column_is_reported_before_writing(tmp_path):
    source, output = tmp_path / "source", tmp_path / "sample"
    shutil.copytree(FIXTURE, source)
    (source / "products.csv").write_text("product_id,product_name\n1,Banana\n")
    with pytest.raises(ValueError, match="products.csv: thiếu cột"):
        make_sample(source, output)
    assert not output.exists()


def test_cli_uses_default_one_percent_and_reports_bad_fraction(tmp_path, capsys):
    output = tmp_path / "sample"
    main(["--input-dir", str(FIXTURE), "--output-dir", str(output)])
    manifest = json.loads(capsys.readouterr().out)
    assert manifest["fraction"] == "0.01"
    assert manifest["sample_users"] == 1
    with pytest.raises(SystemExit) as error:
        main(["--input-dir", str(FIXTURE), "--fraction", "invalid"])
    assert error.value.code == 1
    assert "Lỗi:" in capsys.readouterr().err
