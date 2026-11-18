"""Kiểm tra ranh giới dev/prod và an toàn identifier của DDL tạo schema dev."""

import pytest

from dev_schema_sql import dev_schema_sql


def test_creates_only_personal_schemas():
    ddl = dev_schema_sql("instacart", "hoaitam_")
    assert ddl.splitlines() == [
        f"CREATE SCHEMA IF NOT EXISTS `instacart`.`hoaitam_{layer}`;" for layer in ("bronze", "silver", "gold", "ops")
    ]
    assert ddl == dev_schema_sql("instacart", "hoaitam_")


@pytest.mark.parametrize("prefix", ["", "_", "___", "hoaitam", "x_; DROP SCHEMA bronze; --", "x.y_", "x`_"])
def test_rejects_missing_or_unsafe_prefix(prefix):
    with pytest.raises(ValueError, match="Tiền tố dev"):
        dev_schema_sql("instacart", prefix)


def test_quotes_catalog_and_accepts_hyphenated_user():
    assert "`project``catalog`.`tam-de_bronze`" in dev_schema_sql("project`catalog", "tam-de_")


@pytest.mark.parametrize("catalog", ["", " ", "x\ny", "x\x00y"])
def test_rejects_invalid_catalog(catalog):
    with pytest.raises(ValueError, match="Catalog"):
        dev_schema_sql(catalog, "hoaitam_")
