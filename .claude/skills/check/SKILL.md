---
name: check
description: Chạy toàn bộ kiểm tra local giống workflow pr-checks (terraform fmt/validate/tflint, trivy, ruff, pytest, bundle validate) và tóm tắt kết quả. Dùng trước khi commit hoặc mở PR, hoặc khi người dùng gõ /check.
argument-hint: "[infra|python|bundle]  (bỏ trống = tất cả)"
---

# /check – kiểm tra local như CI

Chạy từ thư mục gốc repo. Nếu có tham số thì chỉ chạy nhóm đó. Ghi lại kết quả từng bước: Đạt / Lỗi / BỎ QUA.

## 1. Terraform (`infra`)

1. `terraform fmt -check -recursive infra`
2. Validate từng thư mục có file `.tf`: `infra/bootstrap`, `infra/modules/naming_tags`, `infra/modules/dbx_workspace`, `infra/stacks/*`. Lệnh cho mỗi thư mục:
   `terraform -chdir=<dir> init -backend=false -input=false >/dev/null && terraform -chdir=<dir> validate -no-color`
3. Nếu đã có `tflint`:
   - Chạy một lần: `tflint --init --config="$PWD/.tflint.hcl"`.
   - Với từng thư mục: `tflint --chdir=<dir> --config="$PWD/.tflint.hcl" --format=compact`.
   - Nếu chưa có: **BỎ QUA – chưa cài tflint** (`brew install tflint`).
4. Nếu đã có `trivy`: `trivy config --severity HIGH,CRITICAL infra`. Chỉ báo kết quả, không tính là lỗi, vì CI đang để exit-code 0.
   Nếu chưa có: **BỎ QUA – chưa cài trivy** (`brew install trivy`).

## 2. Python (`python`)

1. `uv sync --frozen`
2. `uv run ruff check .`
3. `uv run ruff format --check .`
4. `uv run pytest -q`

## 3. Bundle (`bundle`)

- Chưa có lệnh `databricks`: **BỎ QUA – chưa cài databricks CLI** (`brew tap databricks/tap && brew install databricks`).
- Chưa đặt `DATABRICKS_HOST` và chưa có profile (`databricks auth profiles`): **BỎ QUA – chưa có workspace (trước G1)**.
- Các trường hợp còn lại: `cd bundles && databricks bundle validate -t dev`.

## Báo cáo

Trả về **một bảng duy nhất** gồm các cột: nhóm · bước · kết quả · ghi chú. Với mỗi lỗi, trích tối đa 10 dòng log liên quan, kèm `file:dòng`.

- Không tự sửa lỗi, trừ khi người dùng yêu cầu.
- Không chạy `terraform plan` hay bất kỳ lệnh nào cần đăng nhập Azure.
