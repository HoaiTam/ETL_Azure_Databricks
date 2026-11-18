# src/ – quy tắc riêng cho code dữ liệu

Đọc `docs/de/00_tong_quan_de.md` (quy ước chung, chạy lại an toàn) và tài liệu của đúng tầng trước khi viết.

- **Môi trường:** Python 3.11, PySpark 3.5, chạy trên Databricks Runtime 15.4 LTS. Code phải chạy được cả local (pytest, `local[1]`) và trên cluster, nên không phụ thuộc `dbutils` ở hàm thuần.
- **Cấu trúc đích** theo `docs/de/00_tong_quan_de.md` §6:
  - `ingest/`: seed SQL, crawler OFF.
  - `pipelines/batch`, `pipelines/stream`: source của LDP.
  - `tasks/`: các task của job.
  - `common/`: hàm thuần dùng chung, có test.
  - `streaming/simulator/`.
  - Cấu trúc này đã được tạo ở AD-DE-01; chỉ bổ sung logic theo task của từng tầng.
- **Pipeline** dùng `from pyspark import pipelines as dp` (không dùng `import dlt`). Mỗi bảng chỉ thuộc một pipeline.
- **Idempotent là bắt buộc:**
  - Bronze chỉ append, có `pipelines.reset.allowed = false`.
  - Silver upsert theo khóa (`dp.create_auto_cdc_flow`, `event_id`).
  - Gold là materialized view.
  - Không dùng `RunId` hay thời điểm chạy làm tên thư mục landing.
- **Cột kỹ thuật** (`_source_file`, `_ingest_ts`, `_op`, `_lsn`, `source`…) đúng tên trong `docs/de/00` §3.2. Tên bảng và cột nghiệp vụ đúng `docs/data/data_dictionary.md`.
- **Kiểm chất lượng:** bản ghi lỗi đưa vào `silver.quarantine_*`, không xóa im lặng.
- **Bí mật** đọc bằng `dbutils.secrets.get("kv", "<tên>")`. Không đọc từ biến môi trường hay file.
- **Test:**
  - Mỗi hàm thuần trong `common/` có test trong `tests/`, dữ liệu nhỏ trong `tests/fixtures/`.
  - Logic có hai bản (SQL và Python) phải có test so khớp.

## Hàm dùng chung đã có (AD-DE-02)

- `common.sim_time.order_ts_sim(user_id, hour, days_back=0)` trả `datetime` UTC.
  Simulator dùng mặc định `days_back=0` cho đơn cuối train/test. Với đơn prior,
  `days_back` là tổng `days_since_prior_order` của các đơn sau nó trong cùng lịch sử khách hàng.
- `common.sim_time.order_time_sim_sql(orders_table)` dựng truy vấn SELECT cho silver từ bronze:
  có đủ header prior/train/test và `_ingest_ts`. Dùng nguyên câu SQL đã kiểm thử trên local;
  không sao chép công thức vào pipeline. Ví dụ: `spark.sql(order_time_sim_sql("instacart.p2_bronze.orders"))`.
- `common.text.norm(name)` chuẩn hóa tên tiếng Anh để fuzzy match OFF, nhận `None` thành chuỗi rỗng.
  Bỏ từ nhiễu, số đo trước dấu câu để không để lại phần nguyên của số đo thập phân.
- Chạy từ gốc repo: `uv run pytest tests/test_sim_time.py tests/test_norm.py -q`.
  Script Python chạy trực tiếp cần `PYTHONPATH=src` để import `common`.

## Chất lượng dữ liệu dùng chung (AD-DE-03)

- `common.dq` chứa duy nhất `ORDER_RULES`, `ITEM_RULES`, `PRICE_RULES`, `PROMO_RULES`, `EVENT_RULES`.
  Import cùng dict cho expectations và quarantine để tên luật, điều kiện không bị lệch.
- `df.withColumn("_dq_failed", dq_failed(RULES))` tạo `array<string>` tên mọi luật lỗi theo thứ tự dict;
  dòng đạt hoặc dict rỗng nhận `[]`. Lọc `size(_dq_failed) > 0` khi tạo bảng quarantine.
- Điều kiện không được trả NULL. `dq_failed` coi NULL từ luật tùy chỉnh là lỗi; SQL sai/cột thiếu
  vẫn phải báo lỗi. Khoảng cách đơn đầu rỗng và `_rescued_data IS NULL` là các trường hợp hợp lệ.
- Helper không phụ thuộc `pyspark.pipelines`, không khởi tạo session, không chạy action/ghi bảng.
  Cần SparkSession đang chạy khi dựng biểu thức; kiểm được bằng PySpark 3.5 local[1].
- Dữ liệu đầu vào phải có đủ cột đúng kiểu theo luật; parse/cast thuộc bước trước expectations.
  Ví dụ dùng trên pipeline nằm ở `docs/de/03` §4 và `docs/de/04` §4.1.
- Kiểm tra: `uv run pytest tests/test_dq.py -q`.

## Azure SQL nguồn (AD-DE-04)

- Schema/CDC và procedure điều khiển ở `ingest/sql/`; đọc `ingest/sql/README.md` trước khi chạy.
- Chọn CDC (01 → 02 → 03) hoặc timestamp + soft delete (01 → 04) theo spike; không trộn hai nhánh.
  Không tự disable CDC, reset watermark hoặc đổi tier để vượt giới hạn free offer.
- Setter watermark chỉ gọi sau khi công bố landing thành công. Retry cùng run ID/nội dung không nhân log;
  run cũ hoặc bỏ khoảng mốc phải bị từ chối. Seed và ADF không chạy đồng thời.
- Nguồn dùng `updated_at` theo lịch giả lập; nhánh dự phòng lấy `MAX(updated_at)` làm mốc, không dùng giờ máy.
  Seed phải ghi danh sách cột INSERT tường minh và soft delete khi chọn dự phòng.
- Kiểm tích hợp SQL: `python3 scripts/check_retail_sql.py` (cần Docker), tách khỏi pytest/Spark.
  Chỉ dùng database/container tạm; nghiệm thu Azure bằng admin Entra chờ AD-OPS-12 và AD-OPS-06.

## Seed nguồn SQL (AD-DE-05)

- `ingest/seed_retail_db.py`: sinh kế hoạch thuần bằng `generate_wave`, áp dụng bằng `apply_wave`.
  Đọc `ingest/README.md`; wave 0–4 phải tuần tự, giữ nguyên seed/danh mục/chế độ.
- Application lock và marker `etl.seed_wave` nằm cùng transaction với DML. Không commit từng batch,
  không xóa marker để ép retry; không seed cùng ADF. Nhánh soft delete không dùng DELETE vật lý.
- Đường chạy thật chỉ đọc mật khẩu qua `dbutils.secrets.get("kv", "sql-admin-password")`.
  `--dry-run` không cần Spark, SQL hay secret; mặc định cần danh mục đủ 49.688 sản phẩm.
- Job chạy tay `seed_retail_db` trong `jobs_ingest.yml`, khai fixed fields theo policy single-node và G0 #146,
  không lấy default auto-termination. SQL login theo chốt SQL auth + Entra admin; tên dev/prod không tách database.
- Kiểm: `uv run pytest tests/test_seed_retail_db.py -q`; kiểm SQL thật bằng
  `uv run --extra ingest python scripts/check_retail_sql.py --seed` (Docker, cổng loopback tạm, tự dọn).

## Dữ liệu mẫu cho phát triển local

- Fixture nhỏ tự dựng ở `tests/fixtures/instacart/`, gồm đủ sáu CSV; không phải dữ liệu thật.
- Để tạo mẫu 1% từ dữ liệu thật, giải nén sáu CSV vào `data/instacart/`, rồi chạy từ gốc repo:

  ```bash
  python3 scripts/make_sample.py --input-dir data/instacart --output-dir data/sample_1pct
  ```

- Nguồn công khai có đủ sáu CSV: [Instacart trên Zenodo](https://zenodo.org/records/2654444).
  Kiểm checksum từng file theo metadata nguồn trước khi tạo mẫu.

- Script chỉ dùng thư viện chuẩn Python. Chọn `ceil(0.01 × số khách)` bằng seed mặc định 42
  trên danh sách `user_id` đã sắp xếp; cùng dữ liệu và seed thì chọn đúng cùng tập khách.
- Giữ mọi đơn prior/train/test của khách được chọn, mọi món prior/train của các đơn đó;
  đơn test chỉ có header theo dữ liệu gốc. Không lấy mẫu từng dòng món hàng hoặc đổi mã khóa.
- Giữ nguyên toàn bộ `products`, `aisles`, `departments` để bảo toàn khóa ngoại.
- Chỉ giữ tập khách và mã đơn đã chọn trong RAM; hai file món hàng được đọc tuần tự.
- `_manifest.json` ghi tỷ lệ, seed, tập khách, số đơn theo split và số dòng sáu bảng.
  Thư mục mẫu chỉ được công bố sau khi xử lý thành công; đích đã có sẽ bị từ chối để tránh ghi đè.
- Muốn đổi tỷ lệ/seed: thêm `--fraction 0.1 --seed 123` và chọn thư mục đích mới.
- Thử nhanh bằng fixture: `python3 scripts/make_sample.py --input-dir tests/fixtures/instacart
  --output-dir data/sample_fixture --fraction 0.5`. Viết lệnh trên một dòng khi chạy.
- Dữ liệu thật và mẫu nằm trong `data/` đã gitignore; chỉ fixture nhỏ được đưa vào Git.
- `uv sync` cài bộ phát triển; `uv sync --extra ingest` cài thêm Kafka producer và driver SQL.
