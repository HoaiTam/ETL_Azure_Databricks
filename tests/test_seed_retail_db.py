from __future__ import annotations

import json
import sys
from dataclasses import astuple, replace
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from ingest.seed_retail_db import Product, apply_wave, connect_from_secret, generate_wave, load_products, main

PRODUCTS = tuple(Product(i, (i - 1) % 21 + 1) for i in range(1, 49689))


@pytest.mark.parametrize("wave", range(5))
def test_plans_are_deterministic_and_independent_of_product_order(wave):
    forward = generate_wave(PRODUCTS, wave)
    reverse = generate_wave(reversed(PRODUCTS), wave)
    assert forward == reverse
    assert forward.fingerprint == reverse.fingerprint
    assert forward != generate_wave(PRODUCTS, wave, seed=43)


def test_wave_zero_matches_counts_price_ranges_keys_and_anchor():
    plan = generate_wave(PRODUCTS, 0)
    assert len(plan.price_inserts) == 49688
    assert len(plan.promo_inserts) == 3000
    assert not plan.price_updates and not plan.promo_extensions and not plan.promo_deletes
    assert len({p.product_id for p in plan.promo_inserts}) == 3000
    for pid, price, currency, ts in plan.price_inserts:
        dept = PRODUCTS[pid - 1].department_id
        lo, hi = {
            4: (Decimal("0.20"), Decimal("4")),
            5: (Decimal("8"), Decimal("40")),
            7: (Decimal("1"), Decimal("6")),
        }.get(dept, (Decimal("1"), Decimal("10")))
        assert lo <= price <= hi
        assert price * 100 == int(price * 100)
        assert currency == "USD" and ts == datetime(2015, 1, 1)


def test_all_waves_keep_valid_active_promos_and_change_requested_counts():
    active = {}
    seen = set()
    for wave in range(5):
        plan = generate_wave(PRODUCTS, wave)
        ts = datetime(2015, 1, 1) if wave == 0 else datetime(2026, 3, 1 + (wave - 1) * 7)
        if wave:
            assert len(plan.price_updates) == 2484
            assert len({pid for _, _, pid in plan.price_updates}) == 2484
            assert len(plan.promo_inserts) == 300 and len(plan.promo_deletes) == 30 and len(plan.promo_extensions) == 50
            assert not set(plan.promo_deletes) & {key for _, _, key in plan.promo_extensions}
            for factor, when, pid in plan.price_updates:
                assert factor in {Decimal("0.85"), Decimal("0.90"), Decimal("1.05"), Decimal("1.10")}
                assert when == ts and 1 <= pid <= 49688
            for key in plan.promo_deletes:
                del active[key]
            for end, when, key in plan.promo_extensions:
                assert end > active[key].end_date and when == ts
        for promo in plan.promo_inserts:
            assert promo.promo_id not in seen and len(promo.promo_id) <= 10
            assert 1 <= promo.product_id <= 49688
            assert promo.promo_type in {"percent_off", "bogo"}
            assert 3 <= (promo.end_date - promo.start_date).days <= 14
            assert 0 <= promo.discount_pct <= 90 and promo.updated_at == ts
            assert promo.promo_type != "bogo" or promo.discount_pct == 0
            active[promo.promo_id] = promo
            seen.add(promo.promo_id)
        assert len(active) == 3000 + wave * 270


@pytest.mark.parametrize("wave", [-1, 5, True, 1.0])
def test_invalid_wave_is_rejected(wave):
    with pytest.raises(ValueError):
        generate_wave(PRODUCTS, wave)


@pytest.mark.parametrize(
    "products", [[], [Product(1, 1), Product(1, 2)], [Product(0, 1)], [Product(1, 0)], [Product(True, 1)]]
)
def test_invalid_catalog_is_rejected(products):
    with pytest.raises(ValueError):
        generate_wave(products, 0)


def test_catalog_reader_handles_quoted_names_and_validates_header(tmp_path):
    products = tmp_path / "products.csv"
    products.write_text('product_id,product_name,aisle_id,department_id\n2,"Milk, whole",1,4\n1,Apple,2,7\n')
    assert load_products(products) == (Product(1, 7), Product(2, 4))
    products.write_text("product_id,name\n1,Apple\n")
    with pytest.raises(ValueError, match="thiếu"):
        load_products(products)


def test_dry_run_never_reads_secret_or_connects(capsys):
    path = Path(__file__).parent / "fixtures/instacart/products.csv"
    size = len(load_products(path))
    main(["--wave", "0", "--products-csv", str(path), "--expected-products", str(size), "--dry-run"])
    output = json.loads(capsys.readouterr().out)
    assert output["products"] == size and output["price_inserts"] == size
    assert len(output["plan_hash"]) == 64


def test_default_requires_full_catalog_before_any_secret_or_connection():
    path = Path(__file__).parent / "fixtures/instacart/products.csv"
    with pytest.raises(SystemExit):
        main(["--wave", "0", "--products-csv", str(path)])


def test_connection_reads_only_named_key_vault_secret_and_requires_tls(monkeypatch):
    connect = Mock(return_value=object())
    monkeypatch.setitem(sys.modules, "pymssql", SimpleNamespace(connect=connect))
    get = Mock(return_value="test-only-value")
    dbutils = SimpleNamespace(secrets=SimpleNamespace(get=get))
    result = connect_from_secret("test-server", "retail_db", "sqladmin", dbutils)
    get.assert_called_once_with("kv", "sql-admin-password")
    assert result is connect.return_value
    assert connect.call_args.kwargs["password"] == "test-only-value"
    assert connect.call_args.kwargs["encryption"] == "require"
    assert connect.call_args.kwargs["autocommit"] is False
    assert connect.call_args.kwargs["use_datetime2"] is True


def test_input_digest_covers_department_and_not_only_product_id():
    a = generate_wave([Product(1, 4)], 0)
    b = generate_wave([Product(1, 5)], 0)
    assert a.products_hash != b.products_hash
    assert astuple(a.promo_inserts[0])[-1] == datetime(2015, 1, 1)


def test_next_wave_recomputes_fingerprints_of_every_earlier_wave():
    products = PRODUCTS[:200]
    plans = [generate_wave(products, wave) for wave in range(5)]
    for index, plan in enumerate(plans):
        assert plan.previous_plan_hashes == tuple(p.fingerprint for p in plans[:index])
        # Metadata truyền kèm không làm thay đổi marker đã ghi của chính wave.
        assert replace(plan, previous_plan_hashes=()).fingerprint == plan.fingerprint


@pytest.mark.parametrize("changed_wave", [0, 1])
def test_previous_algorithm_drift_is_rejected_before_source_writes(changed_wave):
    products = PRODUCTS[:200]
    plans = [generate_wave(products, wave) for wave in range(3)]
    connection = Mock()
    cursor = connection.cursor.return_value
    history = [(p.wave, p.seed, p.products_hash, p.fingerprint, "cdc") for p in plans[:2]]
    old = history[changed_wave]
    history[changed_wave] = (*old[:3], "0" * 64, old[4])
    cursor.fetchall.return_value = history
    with pytest.raises(ValueError, match="Fingerprint wave trước"):
        apply_wave(connection, plans[2])
    connection.commit.assert_not_called()
    connection.rollback.assert_called_once()
    cursor.executemany.assert_not_called()


def test_connection_cleanup_errors_do_not_hide_the_original_seed_error():
    connection = Mock()
    cursor = connection.cursor.return_value
    original = RuntimeError("mất kết nối khi ghi")
    cursor.execute.side_effect = original
    connection.rollback.side_effect = RuntimeError("rollback không kết nối được")
    cursor.close.side_effect = RuntimeError("close không kết nối được")
    with pytest.raises(RuntimeError) as caught:
        apply_wave(connection, generate_wave(PRODUCTS[:200], 0))
    assert caught.value is original
    connection.commit.assert_not_called()
