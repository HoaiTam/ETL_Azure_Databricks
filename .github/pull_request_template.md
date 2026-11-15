## Thay đổi gì

<!-- 1–3 câu: PR này làm gì và vì sao. -->

Closes #

## Loại thay đổi

- [ ] `infra` (Terraform): stack nào? ______
- [ ] `de-batch` / `de-stream` (src, bundles, adf)
- [ ] `bi` (powerbi)
- [ ] `ds` (src/ml)
- [ ] Tài liệu

## Kiểm tra

- [ ] `pre-commit run --all-files` sạch trên máy
- [ ] Với `infra/**`: đã đọc comment `tf-plan`, không có xóa (destroy) ngoài ý muốn
- [ ] Với `src/**` / `bundles/**`: job `[dev …]` chạy được trên workspace
- [ ] Không có subscription ID, object ID, email, connection string hay key trong diff

## Cổng liên quan

<!-- G0 / G1 / G2 / G3 / G4 / G5 -->
