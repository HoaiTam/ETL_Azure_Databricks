# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Hướng dẫn chung cho mọi agent nằm ở `AGENTS.md`, được nạp ở dòng dưới. File này chỉ bổ sung phần **riêng cho Claude Code**.
Khi sửa quy tắc dùng chung, sửa `AGENTS.md`, không chép sang đây.

@AGENTS.md

---

## Claude Code: kiểm tra đầu việc Databricks

Trước khi làm bất kỳ việc gì liên quan Databricks (`bundles/`, `src/pipelines`, `infra/stacks/30-databricks`, Unity Catalog, job, warehouse, streaming), xem lại danh sách skill khả dụng trong phiên.

1. **Nếu có skill tên `databricks:databricks-core`** (plugin `databricks` đã bật):
   - Nạp `databricks:databricks-core` trước.
   - Sau đó nạp skill sản phẩm phù hợp, ví dụ `databricks:databricks-pipelines` hay `databricks:databricks-dabs`.
2. **Nếu không có:**
   - Dừng việc Databricks và báo người dùng là chưa cài plugin.
   - Đưa người dùng lệnh cài:
     - Gõ trong phiên: `/plugin marketplace add databricks/databricks-agent-skills` rồi `/plugin install databricks@databricks-agent-skills`.
     - Hoặc chạy trong terminal: `claude plugin marketplace add databricks/databricks-agent-skills && claude plugin install databricks@databricks-agent-skills`.
   - Chỉ chạy lệnh terminal khi người dùng đồng ý. Cài xong cần mở phiên mới.
3. **Nếu cần thao tác thật trên workspace** (validate/deploy bundle, xem bảng):
   - Kiểm tra `command -v databricks`. Chưa có CLI thì báo lệnh cài ở `AGENTS.md`.
   - Chưa đăng nhập thì đề nghị người dùng chạy `/databricks:setup`.
   - Chỉ viết code hay YAML thì không cần CLI.

## Claude Code: skill, agent, hook của repo

| Thành phần | Dùng khi |
|---|---|
| `/check [infra\|python\|bundle]` | Trước khi commit hoặc mở PR |
| `/tf-step <stack> <bước>` | Làm tiếp một bước hạ tầng |
| `/gate-check G1` | Kiểm tra cổng đã đủ điều kiện mở chưa |
| Agent `infra-reviewer`, `data-reviewer` | Review độc lập trước khi mở PR |
| Hook `guard_bash.sh` | Chặn các lệnh cấm ở `AGENTS.md` |
| Hook `format_on_edit.sh` | Tự `terraform fmt` / `ruff format` sau mỗi lần sửa file |

**Cấu hình riêng từng máy:** đặt trong `.claude/settings.local.json` (đã gitignore).
- Ví dụ: P1 cần apply `bootstrap` từ máy thì đặt `"env": {"ALLOW_TF_APPLY": "1"}`.

**Ghi chú cá nhân:** đặt trong `CLAUDE.local.md` (đã gitignore).
