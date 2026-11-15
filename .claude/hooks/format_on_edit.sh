#!/usr/bin/env bash
# PostToolUse (Edit/Write): tự format file vừa sửa để khớp pre-commit / pr-checks.
# Không bao giờ chặn: thiếu công cụ hoặc format lỗi thì bỏ qua (exit 0).
file=$(python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("tool_input",{}).get("file_path",""))
except Exception: pass')
[ -n "$file" ] && [ -f "$file" ] || exit 0
cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0

case "$file" in
  *.tf|*.tfvars)
    command -v terraform >/dev/null 2>&1 && terraform fmt "$file" >/dev/null 2>&1
    ;;
  *.py)
    if command -v uv >/dev/null 2>&1; then
      uv run --quiet ruff check --fix "$file" >/dev/null 2>&1
      uv run --quiet ruff format "$file" >/dev/null 2>&1
    fi
    ;;
esac
exit 0
