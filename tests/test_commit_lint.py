from __future__ import annotations

import pytest

from commit_lint import check_header, check_message, check_pr_body


@pytest.mark.parametrize(
    "header",
    [
        "feat(infra): add core storage account with landing container",
        "fix(de): deduplicate order_items on rerun",
        "docs: describe commit and PR conventions",
        "feat(infra)!: rename state container",
        "chore(deps): bump ruff to 0.8.4",
        "ci: lint PR titles and commit messages",
        "fix(stream): handle ADF trigger timeout",
    ],
)
def test_valid_headers(header: str) -> None:
    assert check_header(header) == []


@pytest.mark.parametrize(
    "header",
    [
        "update",  # không có type
        "feat: Add storage",  # viết hoa đầu subject
        "feat(infra): add storage.",  # dấu chấm cuối
        "feature(infra): add storage",  # type sai
        "feat(network): add vnet peering",  # scope sai
        "fix: fix",  # quá chung chung
        "feat(infra): thêm storage account",  # tiếng Việt
        "feat(infra):add storage",  # thiếu khoảng trắng
        "feat(infra): " + "x " * 40,  # quá dài
    ],
)
def test_invalid_headers(header: str) -> None:
    assert check_header(header) != []


def test_message_body_rules() -> None:
    ok = "feat(de): add bronze orders table\n\nAppend-only, reset disabled.\n\nCo-Authored-By: Bot <a@b.c>\n"
    assert check_message(ok) == []
    assert check_message("feat(de): add bronze orders table\nno blank line") != []
    assert check_message("feat(de): add bronze orders table\n\nThêm bảng bronze") != []


def test_git_comments_and_auto_commits_ignored() -> None:
    assert check_message("docs: fix typo in readme\n# Please enter the commit message\n") == []
    assert check_message("Merge branch 'main' into feat/p2-bronze") == []
    assert check_message("fixup! feat(de): add bronze orders table") == []
    assert check_message("fixup! feat(de): add bronze orders table", strict=True) != []


GOOD_BODY = """## Tóm tắt
Thêm storage account cho stack 10-core.

## Đã làm
- Thêm module AVM storage account
- Đánh dấu bước 3.2

## Chưa làm / để PR sau
Không có

## Cách kiểm tra
- `terraform validate`

## Ảnh hưởng
<!-- chi phí -->
Không phát sinh chi phí đáng kể.
"""


def test_pr_body_valid() -> None:
    assert check_pr_body(GOOD_BODY) == []


def test_pr_body_missing_or_empty_sections() -> None:
    assert any("Chưa làm" in e for e in check_pr_body(GOOD_BODY.replace("Không có", "<!-- ghi -->")))
    assert any("Ảnh hưởng" in e for e in check_pr_body(GOOD_BODY.split("## Ảnh hưởng")[0]))
    assert check_pr_body(GOOD_BODY.replace("- Thêm module AVM storage account\n- Đánh dấu bước 3.2", "đã xong")) != []


def test_pr_body_must_be_vietnamese() -> None:
    english = GOOD_BODY.replace("Thêm storage account cho stack 10-core.", "Add storage account for 10-core.")
    assert check_pr_body(english) != []
