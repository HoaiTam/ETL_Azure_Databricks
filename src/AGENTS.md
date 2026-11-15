# src/ – quy tắc riêng cho code dữ liệu

Đọc `docs/de/00_tong_quan_de.md` (quy ước chung, chạy lại an toàn) và tài liệu của đúng tầng trước khi viết.

- **Môi trường:** Python 3.11, PySpark 3.5, chạy trên Databricks Runtime 15.4 LTS. Code phải chạy được cả local (pytest, `local[1]`) và trên cluster, nên không phụ thuộc `dbutils` ở hàm thuần.
- **Cấu trúc đích** theo `docs/de/00_tong_quan_de.md` §6:
  - `ingest/`: seed SQL, crawler OFF.
  - `pipelines/batch`, `pipelines/stream`: source của LDP.
  - `tasks/`: các task của job.
  - `common/`: hàm thuần dùng chung, có test.
  - `streaming/simulator/`.
  - Thư mục `src/batch/` hiện tại là khung tạm, sẽ chuyển sang cấu trúc này.
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
