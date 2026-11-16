# Kết quả spike (P0 → G0)

> Điền theo [01_chuan_bi_va_spike.md](01_chuan_bi_va_spike.md). **Không** ghi subscription ID, object ID hay email vào file này.

Ngày làm spike: `<yyyy-mm-dd>` · Người tổng hợp: P1

## 1. Công cụ đã cài

| Thành viên | az | terraform | tflint | databricks | gh | uv | Docker | Ghi chú |
|---|---|---|---|---|---|---|---|---|
| P1 | 2.91.0 | 1.16.5 | 0.66.0 | 1.26.0 | 2.104.0 | 0.9.21 (Python 3.11.14) | 29.1.3 | macOS arm64, kiểm ngày 2026-11-16. Thêm: trivy 0.76.2, pre-commit 4.7.0, AzCopy 10.34.2, Infracost 2.19.1. tflint cài từ GitHub release. Docker cấp 10 CPU, 15,6 GB RAM, thiếu một chút so với mốc 16 GB cho benchmark DS. `pre-commit run --all-files` chạy được |
| P2 | | | | | | | | |
| P3 | | | | | | | | |
| P4 | | | | | | | | |

## 2. Kết quả phép thử

| # | Phép thử | Sub | Kết quả (Đạt / Không đạt) | Ghi chú / phương án dự phòng đã chọn |
|---|---|---|---|---|
| 3.1 | Region được phép | A, B, C, D | Đạt (Sub A) · Sub B, C, D chưa kiểm | Sub A, kiểm 2026-11-16: policy "Allowed resource deployment regions" chỉ cho `malaysiawest`, `eastasia`, `koreacentral`, `japaneast`, `indiasouthcentral`. Giao với Databricks: `eastasia`, `koreacentral`, `japaneast` (Event Hubs, SQL, ADF, Key Vault, Functions có ở cả 5). Ứng viên Sub A: `eastasia`. Region khác cho Event Hubs nếu Sub B cùng danh sách: `japaneast` hoặc `koreacentral`. Mỗi chủ Sub B/C/D tự chạy lệnh 3.1 |
| 3.2 | Quota vCPU (tổng / theo họ máy) | A | Đạt | Giống nhau ở `eastasia`, `koreacentral`, `japaneast`: tổng regional 6 vCPU (low-priority 3); DSv2 4, DSv3 4, Dv3 4, BS 4; DDSv5, DSv5, EDv4 bằng 0. Một `Standard_DS3_v2` (4 vCPU) dùng gần hết quota, còn 2 vCPU tổng nhưng không đủ cho node DSv2 thứ hai |
| 3.3 | Tạo UAMI | A | Đạt | P1 là Owner của Sub A. Tạo `id-spike` trong `rg-spike` (`eastasia`) được |
| 3.3 | Federated credential cho UAMI | A | Đạt | Subject `repo:<owner>/<repo>:ref:refs/heads/main`. Tên credential phải dài 3–120 ký tự: tên `gh` trong lệnh cũ bị từ chối, dùng `gh-main` |
| 3.3 | Gán role chéo subscription (Sub B → UAMI) | B | Chưa kiểm | Chờ P3 chạy lệnh gán Contributor trên Sub B cho principal của `id-spike` (P1 gửi principal ID riêng, không ghi vào repo). Dự phòng: CI chỉ apply Sub A, Sub B do P3 apply từ máy |
| 3.3 | Tạo App Registration | — | Đạt | Tạo `spike-app` được. Thiết kế vẫn dùng UAMI |
| 3.3 | Tạo nhóm Entra ID | — | Đạt | Tạo `spike-grp` được, nên có thể dùng nhóm Entra cho quyền |
| 3.4 | Tạo workspace Premium | A | | |
| 3.4 | UC tự gán metastore | A | | |
| 3.4 | Quyền account admin / metastore admin | — | | |
| 3.4 | Tạo storage credential từ Access Connector | A | | |
| 3.4 | Serverless SQL warehouse, `SELECT 1` | A | | |
| 3.4 | Serverless jobs / notebook | A | | |
| 3.4 | Cluster single-node DS3_v2, DBR 15.4 LTS | A | | |
| 3.4 | Ngày hết trial Premium | A | | |
| 3.4 | Policy DS: `spark.master` = `local[1]` / `local[2]` | A | | |
| 3.4 | Job `for_each_task` + task value | A | | |
| 3.4 | Model Serving (serverless) | A | | |
| 3.5 | Azure SQL free offer | A | | |
| 3.5 | Event Hubs Standard + Kafka `:9093` | B | | |
| 3.5 | ADF activity "Databricks Job" | A | | |
| 3.5 | ADF tải ZIP Zenodo (ZipDeflate) | A | | |
| 3.5 | Azure Functions Flex Consumption | A | | |
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
| Nơi chạy job DS | Sub C (cùng region) / Sub A ngoài giờ batch | 3.1, 3.4 |
| API model | Model Serving + Function / Function tự tra luật | 3.4, 3.5 |

- [ ] Đã xóa `rg-spike` và workspace thử.
