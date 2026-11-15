<!--
Tiêu đề PR: TIẾNG ANH, dạng `<type>(<scope>): <subject>`, ví dụ `feat(infra): add core storage account`.
Mô tả bên dưới: TIẾNG VIỆT. Năm mục có (*) là bắt buộc, không để trống; không có gì thì ghi `Không có`.
Workflow pr-lint sẽ báo đỏ nếu sai. Quy ước đầy đủ: CONTRIBUTING.md.
-->

## Tóm tắt

<!-- (*) 1–3 câu: PR này làm gì và vì sao. -->

## Đã làm

<!-- (*) Gạch đầu dòng từng việc ĐÃ làm xong và có trong diff. -->

-

## Chưa làm / để PR sau

<!-- (*) Việc trong phạm vi nhưng chưa làm, việc làm dở, giả định chưa kiểm, bước bị BỎ QUA vì thiếu công cụ.
     Người hoặc AI làm tiếp sẽ đọc mục này. Không có thì ghi `Không có`. -->

-

## Cách kiểm tra

<!-- (*) Lệnh đã chạy + kết quả (ví dụ `/check`, `terraform validate`, `uv run pytest -q`).
     Thứ chưa chạy thử thì ghi rõ "chưa kiểm". Bước reviewer cần kiểm tay. -->

-

## Ảnh hưởng

<!-- (*) Tài nguyên Azure/Databricks mới và chi phí ước tính (Azure for Students, 100 USD),
     quyền mới, thay đổi phá tương thích, việc người khác phải làm sau khi merge. -->

-

## Liên quan

Closes #
Cổng: <!-- G0 / G1 / G2 / G3 / G4 / G5 -->

Loại thay đổi:
- [ ] `infra` (Terraform): stack nào? ______
- [ ] `de` / `stream` (src, bundles, adf)
- [ ] `bi` (powerbi)
- [ ] `ds` (src/ml)
- [ ] Tài liệu / CI / cấu hình agent

## Checklist

- [ ] Tiêu đề PR và mọi commit đúng quy ước, viết tiếng Anh
- [ ] `pre-commit run --all-files` (hoặc `/check`) sạch trên máy
- [ ] Với `infra/**`: đã đọc comment `tf-plan`, không có xóa (destroy) ngoài ý muốn
- [ ] Với `src/**` / `bundles/**`: job `[dev …]` chạy được trên workspace
- [ ] Không có subscription ID, object ID, email, connection string hay key trong diff
