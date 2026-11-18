# ADF · CDC retail theo LSN (AD-DE-07)

[DE/01 §3.5–3.6](../docs/de/01_ingest_landing.md) mô tả hợp đồng dữ liệu và cách phục hồi.
Các JSON được dựng theo API và kiểm bằng model Azure SDK.

## Bàn giao cho P1

Linked service `ls_sql_retail` và `ls_adls` đã có. Khi merge, workflow `tf-apply` của stack
`40-orchestration` tự publish hai dataset SQL/Parquet và ba pipeline vào ADF sau bước duyệt của P1.
Publish dùng lại `ds_instacart_adls_binary` với `file_system=landing`.

Trước merge, P1 chạy plan chỉ đọc trên nhánh PR. Kỳ vọng chỉ tạo
`dataset["ds_retail_cdc_sql"]`, `dataset["ds_retail_cdc_parquet"]`,
`pipeline_tier0["pl_cdc_publish_slice"]`, `pipeline_tier1["pl_cdc_one_table"]` và
`pipeline_tier2["pl_ingest_retail_db"]`. Dán tóm tắt plan lên PR. Sau publish, chạy
`pl_ingest_retail_db` từ Monitor khi nguồn và CDC đã sẵn sàng.

## Hợp đồng lúc chạy

- Cha gọi tuần tự `dbo_product_prices`, `dbo_promotions` và chờ mỗi bảng. Run ID cha chỉ dùng cho
  nhật ký và retry stored procedure. Từng bảng có watermark riêng.
- Con dùng Lookup `firstRow` gọi `etl.usp_get_cdc_window` với tham số SQL được bind. Switch nhận
  `empty`, `snapshot`, `window`; mode lạ dừng ngay. `empty` chỉ gọi setter với `rows = 0`.
- Snapshot đọc bảng hiện hành với `_op = 0`, `_lsn = to_lsn` và `_seqval` bằng 0. Window dùng
  `cdc.fn_cdc_get_all_changes_*` với cả hai đầu inclusive và tùy chọn `all`, giữ `_op` 1/2/4.
  Tên bảng/hàm và cột lấy từ danh sách cố định. Hai chế độ có chung tên và kiểu cột CDC, metadata
  gốc `__$…` của snapshot là NULL. Dữ liệu vận hành cần seed xong và CDC bắt kịp trước Lookup.
- Copy ghi một `part-00000.parquet` vào `_staging/retail_db/<bảng>/<phân đoạn>/`. Phân đoạn là
  `snapshot=<to_lsn>` hoặc `window=<from_lsn>_<to_lsn>`, giữ nguyên giá trị hex 22 ký tự.
- Publisher kiểm `rowsRead = rowsCopied`. Có dòng thì Binary Copy sang `retail_db/<bảng>/<phân đoạn>/`
  với kiểm toàn vẹn dữ liệu, xác nhận file và xóa thư mục staging của đúng slice. Không có dòng thì
  dọn thư mục staging nếu connector đã tạo, không công bố file. CDC dùng max LSN toàn database nên một bảng có
  thể có cửa sổ hợp lệ với `rows = 0`.
- Setter chỉ chạy khi publisher thành công. `from_lsn` chỉ có ở `window`. `snapshot` và `empty` bỏ
  hẳn tham số để dùng default NULL. Bảng `etl.run_log` dùng cặp ID lượt ingest và capture instance.
- Mỗi lượt ingest cần chạy riêng với seed và các tiến trình ghi cùng đường dẫn. Bronze chỉ được gọi
  sau khi pipeline cha thành công cả hai bảng.

## Nghiệm thu Azure

1. Trên môi trường nghiệm thu, chạy wave 0 và đợi CDC capture bắt kịp. Xác nhận watermark ban đầu NULL.
   Chạy pipeline cha, ghi ID của cha/con, 2 dòng `snapshot` trong `etl.run_log`, số hàng và 2 file landing.
2. Chạy wave 1, đợi capture. Chạy pipeline lần nữa. Xác nhận 2 log `window` có số dòng dương và Parquet
   `promotions` có cả `_op = 1` và `_op = 4`. Ghi phạm vi LSN và đối chiếu payload với nguồn.
3. Chạy lại khi không seed. Chấp nhận mỗi bảng trả `empty` hoặc `window` với `rows_copied = 0`
   nếu max LSN toàn database đã tiến. Cả hai trường hợp đều không được tạo thư mục landing mới.
4. Trên môi trường debug riêng, gây lỗi Copy hoặc Binary publish sau Lookup. Ghi watermark trước/sau
   lỗi của đúng bảng. Sửa lỗi và dùng **Rerun from failed activity** trên run tương ứng của pipeline con
   với kết quả Lookup và tham số `parent_run_id` cũ. Đối soát số dòng, tên phân đoạn và watermark sau retry.
   Nếu ADF chạy lại cả Switch, các bước dùng cùng phạm vi LSN đã nhận từ Lookup. Nếu ADF chạy Lookup mới,
   phải đối chiếu cửa sổ vì max LSN có thể đã tăng.
5. Nếu publisher đã xóa staging trước khi mất phản hồi, chạy lại publisher từ đầu trong cùng phạm vi.
   Publisher nhận file đích đã tồn tại khi staging vắng mặt. Điều này dựa vào quy tắc chỉ publisher xóa
   staging sau Copy thành công. Giữ nguyên file nếu đang phục hồi và ghi lại bằng chứng vào #43.

Khởi chạy một pipeline cha mới có thể chụp phạm vi LSN khác. Sau lỗi giữa publish và setter, file của
cửa sổ cũ có thể chồng lấn với cửa sổ mới. Trường hợp đó cần đối soát và silver khử trùng theo LSN,
sequence và khóa như tài liệu DE/01. Sau khi quá retention, snapshot phục hồi trạng thái hiện hành.

## Kiểm local

```bash
python3 scripts/build_retail_cdc_adf.py --check
uv run --frozen --with azure-mgmt-datafactory==9.2.0 python scripts/build_retail_cdc_adf.py --check --sdk-check
uv run pytest tests/test_retail_cdc_adf.py -q
```

Chạy script không có `--check` để tái tạo JSON sau khi sửa generator. `--definition <tên>` in resource
API đầy đủ để debug. Test đọc đúng JSON đã commit và mô phỏng dependency, Copy, path và setter trên
local. Việc ADF ghi Parquet thật, thực thi stored procedure và phục hồi từ Monitor cần nghiệm thu Azure.

[Lookup và tham số nguồn SQL](https://learn.microsoft.com/en-us/azure/data-factory/connector-azure-sql-database)
· [Giới hạn lồng activity](https://learn.microsoft.com/en-us/azure/data-factory/concepts-nested-activities)
· [CDC theo khoảng LSN](https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/cdc-fn-cdc-get-all-changes-capture-instance-transact-sql).
