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
