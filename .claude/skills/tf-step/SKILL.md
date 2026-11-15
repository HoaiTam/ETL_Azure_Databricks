---
name: tf-step
description: Thực hiện một bước Terraform trong checklist của một stack (ví dụ "/tf-step 10-core 3.2") đúng theo tài liệu docs/devops, rồi fmt, validate và đánh dấu checklist. Dùng khi người dùng muốn làm tiếp hạ tầng.
argument-hint: "<stack> <bước>   ví dụ: 10-core 3.2 | 30-databricks 3"
---

# /tf-step – làm một bước hạ tầng

Đầu vào:
- `<stack>`: thư mục trong `infra/stacks/`.
- `<bước>`: số bước trong tài liệu.

## Quy trình

1. **Đọc trước khi viết:**
   - Checklist ở đầu `infra/stacks/<stack>/main.tf`.
   - `infra/AGENTS.md`.
   - Mục tài liệu tương ứng:

   | Stack | Tài liệu |
   |---|---|
   | `10-core`, `20-stream`, `90-sandbox` | `docs/devops/03` |
   | `30-databricks` | `docs/devops/05` |
   | `40-orchestration` | `docs/devops/06` §1 |

2. **Kiểm tra điều kiện trước.** Nếu thiếu một trong hai điều dưới đây thì dừng và báo người dùng cần làm gì trước:
   - Các bước trước đã đánh dấu `[x]`.
   - Output cần lấy từ stack khác đã có (ví dụ 30-databricks cần `databricks_workspace_url` từ 10-core).

3. **Chọn version.** Với module AVM hoặc provider:
   - Tìm version mới nhất trên Terraform Registry (`https://registry.terraform.io/modules/Azure/<tên>/azurerm`).
   - Đọc tên tham số trong README của **đúng version đó**.
   - Pin chính xác: `version = "x.y.z"`.
   - Nếu không tra được thì nói rõ và hỏi người dùng.

4. **Viết code:**
   - Resource đặt vào `main.tf` hoặc file theo chủ đề (`storage.tf`, `rbac.tf`…).
   - Thêm `variable` vào `variables.tf`, thêm `output` vào `outputs.tf` nếu stack sau cần dùng.
   - Giá trị không bí mật: thêm vào `infra/envs/<stack>.tfvars`.
   - Biến bí mật hoặc riêng tư: thêm `TF_VAR_*` vào workflow (xem `infra/AGENTS.md`).

5. **Kiểm tra:**
   - `terraform fmt -recursive infra`
   - `terraform -chdir=infra/stacks/<stack> init -backend=false && terraform -chdir=infra/stacks/<stack> validate`

6. **Đánh dấu** `[x]` cho bước vừa làm trong checklist ở `main.tf`.

7. **Soạn mô tả PR** theo `.github/pull_request_template.md`, gồm:
   - Thay đổi gì.
   - Tài nguyên mới và chi phí ước tính (Azure for Students, 100 USD).
   - Quyền mới.
   - Cách kiểm tra, theo checklist cuối stack trong tài liệu.

Không chạy `terraform plan` hay `apply`. Plan sẽ chạy trên PR qua workflow `tf-plan`.
