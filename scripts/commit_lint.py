#!/usr/bin/env python3
"""Kiểm tra commit message, tiêu đề PR và mô tả PR theo CONTRIBUTING.md.

Chỉ dùng thư viện chuẩn, chạy được ở mọi máy có python3 (không cần uv).

Cách dùng:
    python3 scripts/commit_lint.py .git/COMMIT_EDITMSG     # hook commit-msg (pre-commit truyền file)
    python3 scripts/commit_lint.py --message "feat(infra): add core storage"
    python3 scripts/commit_lint.py --title "$PR_TITLE"
    python3 scripts/commit_lint.py --range origin/main..HEAD
    python3 scripts/commit_lint.py --pr-body-file body.md

Exit 0 = hợp lệ, 1 = vi phạm (lý do in ra stderr).
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys

TYPES = ("feat", "fix", "docs", "refactor", "test", "chore", "ci", "build", "perf", "revert", "style")
SCOPES = ("infra", "de", "stream", "bundle", "adf", "bi", "ds", "ci", "docs", "deps", "agents")
MAX_HEADER = 72
MAX_BODY_LINE = 100

HEADER_RE = re.compile(
    r"^(?P<type>[a-z]+)(?:\((?P<scope>[a-z0-9-]+)\))?(?P<breaking>!)?: (?P<subject>.+)$",
)
# Chữ cái chỉ có trong tiếng Việt: dùng để bắt commit/tiêu đề viết tiếng Việt.
VIET_RE = re.compile(r"[àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ]", re.IGNORECASE)
VAGUE_SUBJECTS = {
    "update",
    "updates",
    "fix",
    "fixes",
    "fix bug",
    "fix bugs",
    "bug fix",
    "changes",
    "change",
    "misc",
    "minor",
    "minor changes",
    "stuff",
    "wip",
    "temp",
    "tmp",
    "test",
    "update code",
    "update files",
    "refactor",
    "cleanup",
    "clean up",
}
# Commit do git tự sinh: không bắt theo quy ước.
AUTO_PREFIXES = ("Merge ", 'Revert "')

PR_SECTIONS = ("Tóm tắt", "Đã làm", "Chưa làm", "Cách kiểm tra", "Ảnh hưởng")


def _strip_git_comments(text: str) -> str:
    """Bỏ dòng comment `#` và phần sau dấu cắt của `git commit -v`."""
    text = text.split("# ------------------------ >8 ------------------------", 1)[0]
    return "\n".join(line for line in text.splitlines() if not line.startswith("#")).strip()


def check_header(header: str, *, kind: str = "commit") -> list[str]:
    errors: list[str] = []
    header = header.strip()
    if len(header) > MAX_HEADER:
        errors.append(f"{kind}: dòng đầu dài {len(header)} ký tự, tối đa {MAX_HEADER}.")
    if VIET_RE.search(header) or not header.isascii():
        errors.append(f"{kind}: phải viết bằng tiếng Anh, không dấu (đang có ký tự ngoài ASCII).")

    m = HEADER_RE.match(header)
    if not m:
        errors.append(
            f"{kind}: sai định dạng. Cần `<type>(<scope>): <subject>`, ví dụ `feat(infra): add core storage account`."
        )
        return errors

    if m["type"] not in TYPES:
        errors.append(f"{kind}: type `{m['type']}` không hợp lệ. Dùng một trong: {', '.join(TYPES)}.")
    if m["scope"] is not None and m["scope"] not in SCOPES:
        errors.append(f"{kind}: scope `{m['scope']}` không hợp lệ. Dùng một trong: {', '.join(SCOPES)}.")

    subject = m["subject"]
    if subject != subject.strip():
        errors.append(f"{kind}: subject có khoảng trắng thừa.")
    subject = subject.strip()
    first_word = subject.split()[0] if subject else ""
    if first_word[:1].isupper() and not first_word.isupper():
        errors.append(f"{kind}: subject bắt đầu bằng chữ thường, động từ mệnh lệnh (`add`, không phải `Add`/`Added`).")
    if subject.endswith((".", "!", "?")):
        errors.append(f"{kind}: subject không kết thúc bằng dấu câu.")
    if subject.lower() in VAGUE_SUBJECTS or len(subject.split()) < 2:
        errors.append(f"{kind}: subject quá chung chung (`{subject}`). Nói rõ thay đổi gì, ở đâu.")
    if re.match(r"^(fixup|squash|amend)!", subject, re.IGNORECASE):
        errors.append(f"{kind}: còn commit fixup/squash, hãy gộp trước khi mở PR.")
    return errors


def check_message(message: str, *, strict: bool = False) -> list[str]:
    message = _strip_git_comments(message)
    if not message:
        return ["commit: message rỗng."]
    if message.startswith(AUTO_PREFIXES):
        return []
    lines = message.splitlines()
    header = lines[0]
    if not strict and re.match(r"^(fixup|squash|amend)! ", header):
        return []  # cho phép lúc làm việc trên máy, CI (strict) sẽ chặn
    if strict and re.match(r"^(fixup|squash|amend)! ", header):
        return ["commit: còn commit fixup!/squash!, hãy gộp (rebase --autosquash) trước khi merge."]

    errors = check_header(header)
    if len(lines) > 1 and lines[1].strip():
        errors.append("commit: cần một dòng trống giữa dòng đầu và phần thân.")
    body = "\n".join(lines[1:])
    if VIET_RE.search(body):
        errors.append("commit: phần thân cũng viết bằng tiếng Anh (mô tả tiếng Việt đặt trong PR).")
    for i, line in enumerate(lines[2:], start=3):
        if len(line) > MAX_BODY_LINE and not re.match(r"^\S+://", line.strip()):
            errors.append(f"commit: dòng {i} của thân dài {len(line)} ký tự, tối đa {MAX_BODY_LINE}.")
    return errors


def _section_content(body: str, name: str) -> str | None:
    """Trả nội dung (đã bỏ comment HTML) của mục `## <name>…`, None nếu không có mục."""
    pattern = re.compile(rf"^##\s+{re.escape(name)}[^\n]*\n(?P<content>.*?)(?=^##\s|\Z)", re.MULTILINE | re.DOTALL)
    m = pattern.search(body)
    if not m:
        return None
    return re.sub(r"<!--.*?-->", "", m["content"], flags=re.DOTALL).strip()


def check_pr_body(body: str) -> list[str]:
    errors: list[str] = []
    body = body.replace("\r\n", "\n")
    empty: set[str] = set()
    for name in PR_SECTIONS:
        content = _section_content(body, name)
        if content is None:
            errors.append(f"PR: thiếu mục `## {name}` (dùng .github/pull_request_template.md).")
            empty.add(name)
        elif not content or re.fullmatch(r"[-*\s]*(\.\.\.|…|_+|todo)?[-*\s]*", content, re.IGNORECASE):
            errors.append(f"PR: mục `## {name}` đang trống. Nếu không có gì thì ghi rõ `Không có`.")
            empty.add(name)

    done = _section_content(body, "Đã làm")
    if "Đã làm" not in empty and not re.search(r"^\s*[-*]\s+\S", done, re.MULTILINE):
        errors.append("PR: mục `## Đã làm` cần liệt kê từng việc thành gạch đầu dòng.")
    summary = _section_content(body, "Tóm tắt")
    if "Tóm tắt" not in empty and not VIET_RE.search(summary):
        errors.append("PR: mô tả viết bằng tiếng Việt có dấu (mục `## Tóm tắt` chưa có tiếng Việt).")
    return errors


def _commits_in_range(rev_range: str) -> list[tuple[str, str]]:
    shas = subprocess.run(
        ["git", "rev-list", "--no-merges", "--reverse", rev_range], check=True, capture_output=True, text=True
    ).stdout.split()
    out = []
    for sha in shas:
        msg = subprocess.run(
            ["git", "log", "-1", "--format=%B", sha], check=True, capture_output=True, text=True
        ).stdout
        out.append((sha[:7], msg))
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("file", nargs="?", help="file commit message (hook commit-msg)")
    p.add_argument("--message", help="kiểm tra một commit message")
    p.add_argument("--title", help="kiểm tra tiêu đề PR")
    p.add_argument("--range", dest="rev_range", help="kiểm tra mọi commit trong khoảng, ví dụ origin/main..HEAD")
    p.add_argument("--pr-body-file", help="kiểm tra mô tả PR (markdown)")
    args = p.parse_args(argv)

    errors: list[str] = []
    if args.file:
        with open(args.file, encoding="utf-8") as f:
            errors += check_message(f.read())
    if args.message is not None:
        errors += check_message(args.message)
    if args.title is not None:
        errors += check_header(args.title, kind="PR title")
    if args.rev_range:
        for sha, msg in _commits_in_range(args.rev_range):
            errors += [f"{sha} {e}" for e in check_message(msg, strict=True)]
    if args.pr_body_file:
        with open(args.pr_body_file, encoding="utf-8") as f:
            errors += check_pr_body(f.read())
    if not any((args.file, args.message is not None, args.title is not None, args.rev_range, args.pr_body_file)):
        p.error("cần ít nhất một đầu vào")

    for e in errors:
        print(f"✗ {e}", file=sys.stderr)
    if errors:
        print("Xem quy ước ở CONTRIBUTING.md.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
