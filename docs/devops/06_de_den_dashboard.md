# 06 · Từ pipeline DE đến dashboard Power BI (giai đoạn P6–P7 → cổng G3, G4)

**Đầu vào:**
- G1 đã mở ([05](05_databricks_platform.md)).
- G2 đã mở ([03](03_terraform_azure.md) mục 4) cho phần streaming.
- Quy tắc dữ liệu trong [data_dictionary.md](../data/data_dictionary.md).

**Đầu ra:**
- ADF pipeline và trigger được quản lý bằng code.
- Job batch và job streaming được deploy bằng DAB.
- `silver`/`gold` đạt đối soát.
- Power BI Service có 4 report kèm điều hướng, RLS, bố cục mobile và cập nhật gần real-time.

**Xong khi:** đạt checklist G3 (mục 6.3) và G4 (mục 6.4).

> Tài liệu này chỉ nói phần **triển khai và vận hành** (đặt code ở đâu, deploy thế nào, nghiệm thu ra sao). Logic transform chi tiết thuộc tài liệu DE riêng.

---

## 1. Stack `40-orchestration`: ADF bằng code

**Vì sao dùng Terraform cho ADF thay vì ADF Git integration:**
- ADF Git integration tạo nhánh `adf_publish` và ARM template riêng, dễ xung đột với Terraform (cả hai cùng sở hữu factory).
- Dùng Terraform thì một PR thấy được cả linked service lẫn pipeline, và `drift-check` phát hiện được việc sửa tay trong Studio.

**Cách làm việc của P2:**
1. Thiết kế và debug pipeline trong ADF Studio. Studio cho phép chạy debug mà không cần publish.
2. Chọn *Export JSON* phần `activities` của pipeline, lưu vào `adf/pipelines/<tên>.json`.
3. Mở PR. Merge xong thì `tf-apply` publish pipeline lên factory.
4. Từ lúc này coi Studio là chỉ đọc. Mọi sửa đổi đều đi qua bước 2.

| Resource Terraform | Đối tượng ADF | Ghi chú |
|---|---|---|
| `azurerm_data_factory_linked_service_data_lake_storage_gen2` | `ls_adls` | Xác thực bằng managed identity của ADF |
| `azurerm_data_factory_linked_service_azure_sql_database` | `ls_sql_retail` | Mật khẩu lấy từ Key Vault qua `ls_kv` |
| `azurerm_data_factory_linked_service_key_vault` | `ls_kv` | |
| `azurerm_data_factory_linked_service_web` / HTTP | `ls_zenodo`, `ls_off_api` | Anonymous |
| `azurerm_data_factory_linked_service_azure_databricks` | `ls_dbx` | `msi_work_space_resource_id` (MSI), dùng job cluster hoặc existing cluster theo policy |
| `azurerm_data_factory_dataset_*` | Dataset CSV / Parquet / JSON / SQL | Tham số hóa `ingest_date` |
| `azurerm_data_factory_pipeline` | Pipeline | `activities_json = file("${path.module}/../../../adf/pipelines/x.json")` |
| `azurerm_data_factory_trigger_schedule` | Trigger | `activated = var.triggers_on` (mặc định `false` ở dev để không tốn tiền) |

**Các pipeline:**

| Pipeline | Làm gì | Lịch |
|---|---|---|
| `pl_ingest_instacart` | HTTP Zenodo → `landing/instacart/<bảng>/ingest_date=` (một lần, có thể chạy lại) | Chạy tay |
| `pl_ingest_retail_db` | Lookup watermark `max(updated_at)` → Copy SQL → Parquet `landing/retail_db/` → cập nhật watermark | Hằng ngày |
| `pl_ingest_off` | Gọi job Databricks `off_crawler`. Giới hạn 10 request/phút nên crawler nằm trong Python, không dùng ADF ForEach | Hằng ngày, theo lô khoảng 300 sản phẩm |
| `pl_master` | Execute pipeline retail → OFF → **Databricks Job** `instacart-batch-medallion` | Hằng ngày 02:00 |

`pl_master` gọi job theo **job ID**. Job ID lấy bằng data source `databricks_job` (theo tên job prod do DAB tạo). Vì vậy `40-orchestration` phải apply **sau** lần `bundle deploy -t prod` đầu tiên.

## 2. Job Databricks (DAB)

### 2.1 Job batch `instacart-batch-medallion`

`ingest_bronze → build_silver → build_gold → (train_rules → evaluate_register, thêm ở G5) → refresh_bi`

| Task | Nguồn → đích | Ghi chú triển khai |
|---|---|---|
| `seed_retail_db` (chạy một lần, job riêng) | Sinh `product_prices`, `promotions` có seed → Azure SQL | Đọc mật khẩu từ secret scope `kv` |
| `ingest_bronze` | Volume `raw` → `bronze.*` (Auto Loader `availableNow`, directory listing) | Checkpoint đặt tại `/Volumes/instacart/ops/checkpoints/autoloader/<bảng>` |
| `build_silver` | `bronze.*` → `silver.*` + `silver.quarantine` | DQ ghi vào `ops.dq_results` |
| `build_gold` | `silver.*` → `gold.*` | OPTIMIZE / liquid clustering ở cuối task |
| `refresh_bi` | Kích hoạt refresh semantic model qua REST (nếu được phép); nếu không thì bỏ qua | Xem mục 5 |

### 2.2 Job streaming `instacart-streaming`

`eventhubs_to_bronze → stream_to_silver → pair_counts_gold`

**Ràng buộc quota:** chỉ có 4 vCPU, nên **không chạy cùng lúc** với job batch trên Sub A.
- **Mặc định:** chạy streaming dạng `trigger(availableNow=True)` theo lịch mỗi 10–15 phút. Cách này vẫn là Structured Streaming với checkpoint và exactly-once qua MERGE, chỉ khác là không giữ cluster chạy liên tục.
- **Lúc demo:** tạm dừng lịch batch, chạy `processingTime='30 seconds'` liên tục trong 30–60 phút để thấy dashboard cập nhật trực tiếp.
- **Đo độ trễ (rubric 3.3):** bảng `ops.streaming_latency` gồm:
  - `event_ts` (simulator).
  - `enqueued_ts` (Event Hubs, region B).
  - `bronze_ts`.
  - `silver_ts` (Databricks, region A).
  - So sánh với lần chạy simulator **cùng region** để thấy chênh lệch.

### 2.3 Simulator

Chạy ở máy local hoặc GitHub Codespace. Đọc `order_products__train` rồi phát sự kiện JSON tới Kafka endpoint. Connection string lấy từ Key Vault (`az keyvault secret show`), không lưu trong file. Đây là script Python trong `src/streaming/simulator/`, không phải tài nguyên Terraform.

## 3. Thứ tự triển khai lần đầu

| # | Việc | Ai | Lệnh / workflow |
|---|---|---|---|
| 1 | Deploy bundle prod (tạo job) | P2 | merge PR → `de-deploy` |
| 2 | Chạy `seed_retail_db` một lần | P2 | `databricks bundle run seed_retail_db -t prod` |
| 3 | Apply `40-orchestration` (trigger ở trạng thái tắt) | P1 | merge PR → `tf-apply` |
| 4 | Chạy `pl_ingest_instacart` một lần | P2 | ADF Studio → Trigger now |
| 5 | Chạy `pl_master` → bronze/silver/gold | P2 | Trigger now; kiểm tra G3 |
| 6 | Bật Event Hubs, chạy simulator khoảng 95% tập train | P3 | `stream-toggle up`, simulator |
| 7 | Chạy job streaming tới silver/gold | P3 | `bundle run instacart-streaming` |
| 8 | Bật trigger hằng ngày | P1 | `triggers_on = true` qua PR |
| 9 | Dựng Power BI (mục 5) | P4 | — |

## 4. Lớp phục vụ (serving) cho Power BI: thang dự phòng

| Ưu tiên | Phương án | Khi nào dùng | Hạ tầng thêm |
|---|---|---|---|
| **1 (mặc định)** | Serverless SQL warehouse `wh-instacart-bi` → connector *Azure Databricks* (DirectQuery cho bảng streaming, Import cho bảng lớn ít đổi) | Spike 3.4 đạt serverless | Không (đã có ở `30-databricks`) |
| 2 | Task `publish_serving` ghi các bảng gold nhỏ (KPI, top N) sang **Azure SQL** `serving_db` qua JDBC → Power BI DirectQuery/Import Azure SQL | Không có serverless | Thêm DB `serving_db` ở `10-core` (dùng chung server) |
| 3 | Power BI đọc thẳng Delta trên ADLS bằng hàm `DeltaLake.Table` của Power Query (Import) | Hai phương án trên đều không dùng được | Quyền Storage Blob Data Reader cho tài khoản Power BI |

Bảng gold dành cho Power BI phải **nhỏ và đã tổng hợp sẵn**. Không cho Power BI quét `silver.order_items` (32 triệu dòng) mỗi lần mở trang.

## 5. Power BI Service: đáp ứng rubric mục 4 "Trực quan dữ liệu"

### 5.1 Workspace, chia sẻ, giấy phép

- Workspace `instacart-bi`: P4 là Admin, P1–P3 là Member.
- Phân phối cho người xem bằng **Power BI App** "Instacart Analytics", chia thành audience theo nhóm người xem.
- **Mọi người xem** (kể cả giảng viên) cần giấy phép Pro hoặc trial, vì workspace không nằm trên Premium/Fabric capacity. Gửi lời mời trước buổi demo.
- **Không** dùng *Publish to web*: tính năng này công khai link và bỏ qua RLS, trái với yêu cầu bảo mật.
- Nguồn report lưu dạng **PBIP** trong `powerbi/` (text, review được qua PR). File `.pbix` không commit.

### 5.2 Bốn report và điều hướng (1 điểm)

| Trang | Nội dung | Bảng gold |
|---|---|---|
| 1 · Tổng quan | KPI: số đơn, số sản phẩm/giỏ, tỷ lệ mua lại, doanh thu giả lập; xu hướng theo `order_ts_sim` | `kpi_daily`, `kpi_by_dow_hour` |
| 2 · Chi tiết sản phẩm | Drill-through từ một sản phẩm: giá hiện hành, khuyến mãi, Nutri-Score, ảnh OFF, sản phẩm hay mua kèm | `product_360`, `product_pair_counts` |
| 3 · Bộ lọc 1: thời gian mua | Heatmap thứ × giờ (`order_dow`, `order_hour_of_day` gốc), slicer khoảng ngày | `kpi_by_dow_hour` |
| 4 · Bộ lọc 2: danh mục | Slicer department/aisle → doanh thu, mua lại, top sản phẩm | `department_sales` |
| (5 · Vận hành streaming) | Số sự kiện/phút, độ trễ p50/p95, đối soát 131.209 đơn | `ops.streaming_latency`, `ops.dq_results` |

Điều hướng gồm:
- Thanh *Page navigator* trên mọi trang.
- Nút *Back*.
- Nút bookmark ("Xem theo tuần / theo giờ").
- Drill-through từ trang 1, 4 sang trang 2.

### 5.3 Cập nhật tức thời từ mô hình dữ liệu (0.5 điểm)

| Cơ chế | Độ trễ thấy được | Giới hạn |
|---|---|---|
| DirectQuery tới bảng gold streaming | Mỗi lần mở hoặc tương tác trang là dữ liệu mới nhất | — |
| Automatic page refresh (APR) | Trên workspace Pro (shared capacity), **tối thiểu khoảng 30 phút**; trong Desktop thì xuống được vài giây | Không đủ "tức thời" khi chỉ dùng APR trên Service |
| **Push semantic model (streaming tile)** | Vài giây | Microsoft đã thông báo ngừng *tạo mới* real-time semantic model từ **31/10/2027**, nên đồ án vẫn dùng được. Cần kiểm tra lại ở spike 3.6 |

**Cách triển khai push:**
1. P4 tạo *streaming semantic model* kiểu API và lấy **push URL**. URL này chứa key, nên lưu vào Key Vault với tên `pbi-push-url`.
2. Task `pair_counts_gold` trong `foreachBatch` gửi POST các KPI tổng hợp của micro-batch (số đơn, doanh thu giả lập, top cặp) tới push URL.
3. Ghim tile streaming lên **dashboard** Power BI cạnh các visual của report.

Trong buổi demo, khi phát nốt khoảng 5% đơn còn lại, tile sẽ nhảy số sau vài giây. Các trang DirectQuery thì cập nhật khi bấm *Refresh visuals*.

### 5.4 Bảo mật: chia sẻ và RLS (1 điểm)

- **RLS động** trong semantic model:
  - Bảng `gold.bi_user_access(user_email, department_id)` do job DE seed.
  - Role `DeptManager` có điều kiện lọc `[user_email] = USERPRINCIPALNAME()`, lan qua quan hệ tới `department_sales`.
  - Role `Executive` không lọc.
- Gán thành viên hoặc nhóm vào role trong Service (Semantic model → Security).
- Kiểm tra bằng *Test as role / View as*. Chụp màn hình hai người dùng thấy hai bộ số khác nhau để đưa vào báo cáo.
- **Hai lớp bảo vệ:**
  - Power BI RLS lọc dòng theo người xem.
  - Unity Catalog chỉ cho tài khoản kết nối của Power BI đọc schema `gold` (tài liệu 05, bước 5). Người xem không bao giờ chạm được tới `silver`/`bronze`.

### 5.5 Responsive mobile (0.5 điểm)

Mỗi trang có **Mobile layout** riêng (View → Mobile layout): sắp lại KPI card dạng dọc, ẩn visual phụ. Kiểm tra trên app Power BI Mobile, chụp màn hình cho báo cáo.

### 5.6 Làm mới dữ liệu

| Phần | Chế độ | Lịch |
|---|---|---|
| Bảng streaming / KPI | DirectQuery | Theo tương tác, có thêm APR |
| Dim sản phẩm, bảng lớn ít đổi | Import | Scheduled refresh 1 lần/ngày, sau `pl_master` (Pro cho tối đa 8 lần/ngày) |
| Tile real-time | Push | Liên tục khi job streaming chạy |

Thông tin đăng nhập nguồn trên Service: OAuth bằng tài khoản P4, hoặc một tài khoản kỹ thuật chỉ có quyền đọc `gold`. Ghi rõ lựa chọn trong báo cáo.

## 6. Checklist nghiệm thu từng cổng

### 6.1 G1: DE trên cloud (lặp lại từ 05 để tiện theo dõi)
- [ ] ADF chạy `pl_ingest_instacart`: đủ 6 file CSV trong `landing/instacart/`.
- [ ] `bronze.*` có số dòng **bằng** file nguồn: `orders` 3.421.083; `order_products__prior` 32.434.489; `products` 49.688; `aisles` 134; `departments` 21.

### 6.2 G2: streaming
- [ ] Simulator phát 1.000 sự kiện thử → `bronze.order_events` có đủ 1.000 bản ghi, không trùng `event_id`.
- [ ] Chạy lại cùng 1.000 sự kiện → silver **không** tăng số dòng (MERGE idempotent).

### 6.3 G3: silver đạt đối soát → **DS bắt đầu**
- [ ] `silver.orders`: đơn prior + test lấy từ batch, đơn train lấy từ stream. Tổng = 3.421.083 sau khi replay xong.
- [ ] Phần stream: **131.209 đơn**, **1.384.617 dòng** `order_items` có `source = 'stream'`.
- [ ] `silver.dim_product` = **49.688 dòng** (LEFT JOIN không làm mất hay nhân dòng).
- [ ] `silver.baskets` chỉ có split `prior | holdout`, không có đơn test (đơn test không có món).
- [ ] `silver.quarantine` có số dòng được giải thích được (ghi trong `ops.dq_results`).
- [ ] Ghi Delta version của `silver.baskets` vào tài liệu để DS dùng làm mốc cố định.

Đạt hết thì P4 dựng `90-sandbox` (hoặc dùng Sub A ngoài giờ batch) và bắt đầu FP-Growth.

### 6.4 G4: dashboard đạt rubric
- [ ] App Power BI có ≥ 4 trang, điều hướng bằng nút và page navigator.
- [ ] Hai tài khoản ở hai role RLS thấy số liệu khác nhau (có ảnh chụp).
- [ ] Mobile layout cho đủ 4 trang (có ảnh chụp trên điện thoại).
- [ ] Phát nốt khoảng 5% đơn → tile streaming tăng trong ≤ 1 phút; trang DirectQuery cập nhật sau khi refresh.
- [ ] Bảng `ops.streaming_latency` có số liệu cùng region và khác region (rubric 3.3).
- [ ] Bảng `ops.benchmark_results`: CSV, Parquet, Delta, Delta có OPTIMIZE (rubric 3.1, 3.4).
- [ ] Bảng chi phí theo tag `owner`/`stack` trong Cost Management + ảnh chụp quota vCPU (rubric 5.4).

## 7. Sau G4: chuẩn bị cho DS (G5, viết ở tài liệu DS)

Hạ tầng sẽ cần thêm:
- MLflow experiment trong `/Shared/instacart`, artifact lưu ở container `mlflow`.
- Model đăng ký vào UC tại `instacart.ml.*`.
- Model serving endpoint (serverless, nếu dùng được).
- Azure Function (FaaS) làm API proxy cho trang web gọi model.
- Bảng `ml.experiment_results` để Power BI vẽ kết quả theo tham số (`min_support`, `min_confidence`, số partition).

Các thành phần này sẽ được thêm thành bước mới trong `10-core`, `30-databricks` và `bundles/`, theo cùng quy trình PR → plan → apply.
