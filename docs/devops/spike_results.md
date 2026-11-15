# Kết quả spike (P0 → G0)

> Điền theo [01_chuan_bi_va_spike.md](01_chuan_bi_va_spike.md). **Không** ghi subscription ID, object ID hay email vào file này.

Ngày làm spike: `<yyyy-mm-dd>` · Người tổng hợp: P1

## 1. Công cụ đã cài

| Thành viên | az | terraform | tflint | databricks | gh | uv | Docker | Ghi chú |
|---|---|---|---|---|---|---|---|---|
| P1 | | | | | | | | |
| P2 | | | | | | | | |
| P3 | | | | | | | | |
| P4 | | | | | | | | |

## 2. Kết quả phép thử

| # | Phép thử | Sub | Kết quả (Đạt / Không đạt) | Ghi chú / phương án dự phòng đã chọn |
|---|---|---|---|---|
| 3.1 | Region được phép | A, B, C, D | | |
| 3.2 | Quota vCPU (tổng / theo họ máy) | A | | |
| 3.3 | Tạo UAMI | A | | |
| 3.3 | Federated credential cho UAMI | A | | |
| 3.3 | Gán role chéo subscription (Sub B → UAMI) | B | | |
| 3.3 | Tạo App Registration | — | | |
| 3.3 | Tạo nhóm Entra ID | — | | |
| 3.4 | Tạo workspace Premium | A | | |
| 3.4 | UC tự gán metastore | A | | |
| 3.4 | Quyền account admin / metastore admin | — | | |
| 3.4 | Tạo storage credential từ Access Connector | A | | |
| 3.4 | Serverless SQL warehouse, `SELECT 1` | A | | |
| 3.4 | Serverless jobs / notebook | A | | |
| 3.4 | Cluster single-node DS3_v2, DBR 15.4 LTS | A | | |
| 3.4 | Ngày hết trial Premium | A | | |
| 3.5 | Azure SQL free offer | A | | |
| 3.5 | Event Hubs Standard + Kafka `:9093` | B | | |
| 3.5 | ADF activity "Databricks Job" | A | | |
| 3.5 | ADF tải ZIP Zenodo (ZipDeflate) | A | | |
| 3.6 | Giấy phép Power BI Pro / trial | — | | |
| 3.6 | Workspace `instacart-bi` + chia sẻ | — | | |
| 3.6 | Connector Azure Databricks trong Desktop | — | | |
| 3.6 | Push / streaming semantic model | — | | |
| 3.6 | Auto page refresh trên Pro | — | | |
| 3.6 | Service principal gọi Power BI REST API | — | | |

## 3. Quyết định G0

| Quyết định | Chốt | Lý do (phép thử) |
|---|---|---|
| Region Sub A | | 3.1, 3.5 |
| Region Sub B (khác Sub A) | | 3.1, 3.5 |
| Region sandbox Sub C/D | | 3.1, metastore |
| Cách CI đăng nhập | UAMI + OIDC / chỉ check tĩnh | 3.3 |
| Unity Catalog | External location / managed storage | 3.4 |
| Lớp serving Power BI | Serverless warehouse / Azure SQL / `DeltaLake.Table` | 3.4, 3.6 |
| Compute | Single-node DS3_v2 / máy khác 4 vCPU | 3.2, 3.4 |

- [ ] Đã xóa `rg-spike` và workspace thử.
