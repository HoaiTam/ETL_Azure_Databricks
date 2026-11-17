# Azure SQL nguồn · AD-DE-04

Schema theo [DE/01 §3](../../../docs/de/01_ingest_landing.md) và
[data dictionary §3](../../../docs/data/data_dictionary.md). Các script không tạo database,
firewall, tài khoản hay quyền cloud; các phần đó thuộc AD-OPS-12.

## Điều kiện trước khi chạy trên Azure

- AD-OPS-12 (#14) cấp `retail_db`, firewall và admin Entra. Spike AD-OPS-06 (#8, PR #141)
  đã xác nhận CDC free offer; vẫn cần nghiệm thu trên database chính thức, không dùng kết quả spike thay thế.
- Dùng đúng admin Entra của server, kết nối **retail_db**, không phải `master`.
  Bật CDC trên Azure SQL cần quyền `db_owner`; ADF chỉ dùng quyền đọc nguồn/CDC và EXECUTE
  các procedure cần thiết do P1 cấp, không dùng admin cho pipeline.
- Không có seed hoặc ADF đang chạy trong lúc cài đặt. Một người chạy tuần tự, dừng ở lỗi đầu tiên.
- Đây là script khởi tạo cho schema của dự án, không phải công cụ tự migrate schema đã bị sửa tay.
  Bảng hiện có phải đúng kiểu cột/khóa ở `01_schema.sql`; kiểm drift trước nếu tái dùng database cũ.

## Chọn đúng một nhánh

| Nhánh | Thứ tự script | Mốc và log |
|---|---|---|
| CDC theo transaction log | `01_schema.sql` → `02_enable_cdc.sql` → `03_etl_control.sql` | `etl.cdc_watermark`, `etl.run_log` |
| Dự phòng khi spike không đạt | `01_schema.sql` → `04_fallback_watermark.sql` | `etl.ts_watermark`, `etl.ts_run_log` |

Có thể chạy lại cùng nhánh: tạo bảng/cột/mốc còn thiếu, `CREATE OR ALTER` procedure,
giữ nguyên dữ liệu nguồn, watermark và log. **Không chạy cả hai nhánh**; script từ chối trộn chế độ.
Không tự disable CDC, xóa bảng hay reset mốc khi gặp lỗi.

Ví dụ từ gốc repo với **sqlcmd (Go)**, sau khi đăng nhập Azure CLI bằng admin Entra đúng tenant.
`RETAIL_SQL_SERVER` do người vận hành đặt vào shell, không ghi giá trị thật vào Git:

```bash
sqlcmd -S "$RETAIL_SQL_SERVER" -d retail_db --authentication-method ActiveDirectoryDefault -Nm -b -i src/ingest/sql/01_schema.sql
sqlcmd -S "$RETAIL_SQL_SERVER" -d retail_db --authentication-method ActiveDirectoryDefault -Nm -b -i src/ingest/sql/02_enable_cdc.sql
sqlcmd -S "$RETAIL_SQL_SERVER" -d retail_db --authentication-method ActiveDirectoryDefault -Nm -b -i src/ingest/sql/03_etl_control.sql
```

Kiểm phiên bản client hỗ trợ `-Nm` (mã hóa bắt buộc), không dùng `-C` để bỏ kiểm chứng chỉ Azure.
**Chỉ chạy bằng `sqlcmd -b`**, hoặc dùng công cụ đã bật **SQLCMD mode** và thêm `:On Error exit`
trước khi chạy toàn file. Chế độ SQL thông thường chỉ hỗ trợ `GO` chưa đủ: sau `THROW` ở một batch,
client có thể vẫn chạy các batch sau và tạo procedure của nhánh bị từ chối. Không chạy toàn file ở chế độ đó.
`GO` là dấu phân batch của client, không gửi nguyên file qua một lần `cursor.execute()`.
Cách xác thực: [Microsoft Learn về sqlcmd Entra](https://learn.microsoft.com/en-us/sql/tools/sqlcmd/sqlcmd-authentication),
[tùy chọn sqlcmd](https://learn.microsoft.com/en-us/sql/tools/sqlcmd/sqlcmd-utility).

Chỉ khi spike quyết định dự phòng mới thay hai lệnh cuối bằng `04_fallback_watermark.sql`.
Không tự đổi tier hoặc cho phép tính phí vượt free offer để bật CDC.

## Hợp đồng CDC với ADF (AD-DE-07)

```sql
EXEC etl.usp_get_cdc_window @capture_instance = N'dbo_product_prices';
EXEC etl.usp_get_cdc_window @capture_instance = N'dbo_promotions';
```

Mỗi lần Lookup trả **một dòng** `mode`, `from_lsn`, `to_lsn`:

| mode | Ý nghĩa | `from_lsn` | `to_lsn` |
|---|---|---|---|
| `snapshot` | Chưa có mốc hoặc cửa sổ kế tiếp đã quá retention | NULL | LSN cao nhất đã capture |
| `window` | Có cửa sổ CDC mới; cả hai đầu inclusive | `increment(last_lsn)` | LSN cao nhất đã capture |
| `empty` | Chưa có LSN mới | NULL | Bằng watermark hiện tại |

LSN là hex cố định **22 ký tự**, dạng `0x` + 20 chữ số hex, chuẩn hóa bởi SQL.
`last_lsn` lưu `binary(10)`; procedure không chấp nhận LSN 0/NULL/sai độ dài, capture instance lạ,
hoặc mốc vượt timeline. Min/max LSN chưa sẵn sàng hay instance không truy cập được là **lỗi để retry**,
không phải `empty`. Kiểm retention theo `increment(last_lsn) < min_lsn`, vì đó mới là đầu cửa sổ cần đọc.

`etl.usp_set_cdc_watermark` nhận `@capture_instance`, `@to_lsn`, `@adf_run_id`, `@mode`,
`@from_lsn = NULL`, `@rows = NULL`. Truyền nguyên output Lookup, không tính lại LSN sau Copy:

- Chỉ gọi sau Copy → công bố thư mục landing tất định → xóa staging đều thành công.
- `window` bắt buộc có `from_lsn`; với activity Stored Procedure của `snapshot` và `empty`,
  **bỏ hẳn tham số `from_lsn` khỏi `storedProcedureParameters`** để dùng mặc định SQL NULL.
  Không map `firstRow.from_lsn` thành String vì ADF có thể gửi chuỗi rỗng (setter từ chối với 51003).
  `empty` có `rows = 0` hoặc NULL,
  chỉ ghi log, không đổi watermark hay thời điểm cập nhật mốc.
- Một transaction khóa dòng watermark theo capture instance, cập nhật mốc và ghi log nguyên tử.
- Khóa log `(adf_run_id, capture_instance)` cho phép retry cùng tham số là no-op,
  kể cả sau một run mới hơn; cùng run ID nhưng dữ liệu khác bị từ chối.
- Run khác dùng cửa sổ cũ/bỏ khoảng trống/hạ mốc bị từ chối; phải chạy Lookup lại.
  Đặt ADF concurrency = 1 cho từng bảng nguồn. Khóa tại bước ghi không thay thế việc điều phối Copy.

**Giới hạn snapshot:** không chạy seed cùng ADF; chờ capture bắt kịp sau mỗi đợt seed trước Lookup.
Snapshot đọc bảng hiện hành, không phải ảnh lịch sử được SQL ghim chính xác ở `to_lsn`.
Snapshot sau mất retention không tái tạo mọi phiên bản trung gian hoặc các hard delete đã mất;
log ghi mode snapshot để nhận biết, cần đối soát và xử lý lịch sử ở bước vận hành.

## Nhánh dự phòng: updated_at và soft delete

`04_fallback_watermark.sql` thêm `is_deleted bit NOT NULL DEFAULT 0`, với giá trị 0 cho dòng cũ.
Không bật CDC. Dùng `etl.usp_get_ts_window` / `etl.usp_set_ts_watermark` với cùng tên capture instance.
Lookup trả `mode`, `from_ts`, `to_ts` kiểu `datetime2(7)`:

- Lần đầu `snapshot`, đọc toàn bảng, **giữ cả tombstone** `is_deleted = 1`.
- `window`: đọc `WHERE updated_at > @from_ts AND updated_at <= @to_ts`.
- Không có đợt mới: `empty`, chỉ ghi log. Nguồn rỗng lần đầu dùng mốc `0001-01-01`.
- Setter nhận `@capture_instance, @to_ts, @adf_run_id, @mode, @from_ts = NULL, @rows = NULL`.
  `snapshot` truyền `from_ts = NULL`; `window` và `empty` truyền đúng `from_ts` từ Lookup.
  Setter cũng khóa mốc, rollback khi lỗi và không nhân log khi retry.

**Mốc được lấy từ `MAX(updated_at)` của từng bảng**, khác ví dụ dùng giờ máy ở thiết kế ban đầu.
Nguồn dự án dùng lịch giả lập: đợt 0 là 2015, đợt 1–4 là 03/2026.
Dùng `SYSUTCDATETIME()` làm mốc vào tháng 11/2026 sẽ bỏ qua các đợt seed tiếp theo.

Điều kiện để nhánh này đúng: mỗi đợt seed là một transaction, thời gian đợt mới **lớn hơn** mốc đã công bố,
không seed đồng thời với ADF từ Lookup đến khi ghi watermark. Timestamp trùng mốc của đợt cũ/backdate
không được phát hiện; không dùng cách này cho nguồn ghi đồng thời tùy ý. Nhánh này chỉ lưu trạng thái
cuối giữa các lần Copy, không giữ mọi lần thay đổi như CDC log-based.

Seed (AD-DE-05) phải INSERT danh sách cột tường minh; khi hủy bản ghi dùng ví dụ:

```sql
UPDATE dbo.promotions
SET is_deleted = 1, updated_at = @wave_ts
WHERE promo_id = @promo_id;
```

Không hard delete. Đồng hồ `updated_at` có thể là lịch giả lập; `updated_at` của bảng điều khiển và
`finished_at` của log luôn là giờ vận hành UTC. ADF/bronze/silver cần chọn hợp đồng timestamp và
`is_deleted` theo nhánh này ở task tương ứng; không giả vờ phát ra LSN CDC.

## Kiểm thử local và CI

Có Docker đang chạy, Python 3.11+, khoảng 3 GB RAM cho container và dung lượng tải image:

```bash
python3 scripts/check_retail_sql.py
```

Script tạo container tên ngẫu nhiên, pin SQL Server **2022 CU28 Developer** bằng digest,
không publish port, sinh mật khẩu tạm trong bộ nhớ, tạo hai database test trắng và tự xóa container
trong `finally`. Không đụng container/database có sẵn hoặc Azure; image được giữ trong cache Docker.
SQL Server image là amd64; máy Apple Silicon cần giả lập Docker (đã kiểm local, không thay thế chứng nhận hỗ trợ của Microsoft).

- `tests/sql/test_retail_cdc.sql`: chạy lại trước/sau khi có dữ liệu, retention 7 ngày, snapshot cho cả
  hai capture instance, empty, cửa sổ liên tiếp, insert/update/delete thật, retry kể cả run cũ,
  LSN sai, run ID xung đột, rollback khi ghi log lỗi, snapshot sau retention và capture instance bị gỡ.
- `tests/sql/test_retail_fallback.sql`: nguồn rỗng, snapshot/window/empty theo lịch giả lập,
  soft delete, retry, mốc cũ/vượt nguồn và chạy lại không mất dữ liệu.
- SQL fixture từ chối database không có tiền tố `retail_db_test_` hoặc không trắng.
  Không chạy trực tiếp lên database dùng chung. Đây là regression riêng, **không nằm trong pytest**.
- Workflow `retail-sql-checks` chạy cùng lệnh khi PR sửa SQL, fixture hoặc runner/workflow.
  Local tắt SQL Agent và gọi scan thủ công để test tất định; Azure SQL có scheduler riêng.

## Nghiệm thu Azure còn chờ

Sau khi #14 hoàn tất, chạy nhánh CDC đã đạt spike #8/#141 bằng admin Entra rồi chạy lại lần hai.
Trước lần ADF đầu, kiểm nguồn/mốc chưa có dữ liệu cũ; nếu mốc đã tồn tại, không tự reset để ép snapshot.
Với CDC, lưu kết quả `is_cdc_enabled`, hai dòng `cdc.change_tables`, retention từ `sys.sp_cdc_help_jobs`
và hai lệnh Lookup trả `snapshot` (chờ scheduler sẵn sàng rồi thử lại nếu LSN chưa sẵn sàng).
Nếu chọn dự phòng, ghi kết quả spike và nghiệm thu hợp đồng timestamp thay vì đánh dấu CDC đạt.
Không đưa thông tin tenant/tài khoản/kết nối riêng vào ảnh/log commit.

Tham chiếu chính thức:
[CDC trên Azure SQL](https://learn.microsoft.com/en-us/azure/azure-sql/database/change-data-capture-overview),
[min LSN và thiếu quyền](https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-cdc-get-min-lsn-transact-sql),
[đổi retention](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-change-job-transact-sql).
