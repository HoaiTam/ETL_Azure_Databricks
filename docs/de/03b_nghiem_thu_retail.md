# Nghiệm thu silver retail — AD-DE-11

Pipeline `instacart_batch` tự nạp `silver_retail.py` qua glob của AD-DE-08/09. Không tạo pipeline,
job hay cluster mới; chỉ thêm configuration session UTC. Tên catalog/bronze/silver lấy từ bundle,
dev vẫn ghi vào schema `<user>_silver`, prod ghi `silver`.

| Dataset | Vai trò |
|---|---|
| product_prices_valid / promotions_valid | Temporary view: chuẩn hóa, expectations drop dữ liệu không đạt |
| product_prices_changes / promotions_changes | Temporary view: loại bản sao sau DQ, chỉ giữ cột nguồn CDC cần thiết |
| silver.product_prices_history | AUTO CDC SCD2, key product_id, sequence updated_at, track unit_price/currency, CDF bật |
| silver.promotions | AUTO CDC SCD1, key promo_id, sequence struct(_lsn,_seqval), delete khi op=1 |
| silver.quarantine_product_prices | Bản ghi giá không đạt, raw payload + _dq_failed + _quarantined_at |
| silver.quarantine_promotions | Tương tự cho khuyến mãi, để dữ liệu bị drop không mất dấu |

## Chuẩn hóa và DQ

- Hợp đồng đầu vào là CDC log: snapshot op=0, delete=1, insert=2, update-after=4; before-image=3
  không được áp dụng như trạng thái mới, vẫn giữ ở bronze. Op thiếu/sai được quarantine.
- LSN/seqval nhận binary dài đúng 10 byte hoặc hex `0x`/`0X` + đúng 20 chữ số, không phân biệt hoa/thường;
  chuẩn hóa về binary để sắp thứ tự nhất quán. Không tự tạo LSN giả khi thiếu. Nhánh timestamp/soft-delete
  dự phòng cần mapping/sequence được thiết kế riêng, chưa được suy đoán tự động trong flow này.
- Dùng try_cast dưới ANSI, giá DECIMAL(10,2), ngày DATE, thời gian TIMESTAMP_LTZ. Session pipeline UTC
  để datetime2 không kèm múi giờ được hiểu theo quy ước nguồn. Không đổi currency sang USD hay tự sửa payload sai.
- Các business rules lấy từ common.dq; common.retail chỉ bổ sung kiểm transport/khóa/thứ tự và bọc
  điều kiện delete. Giá delete vào quarantine với luật `no_price_delete`; khuyến mãi delete vẫn cần
  promo_id/LSN/seqval và có thể thiếu discount/window/updated_at. Rescue và dữ liệu thiếu khóa/sequence không được bỏ qua.
- Valid view áp expectations trước, view changes mới dedup. Giá gộp cùng business state/time, với snapshot và upsert chồng lấn được coi cùng thao tác; promo gộp cùng toàn bộ payload/op/log position. Metadata file/ingest không phải
  khóa dedup và không được ghi vào hai bảng AUTO CDC. Bản ghi lỗi giữ nguyên raw trong quarantine.

## Hợp đồng thứ tự và lịch sử

Giá dùng updated_at để `__START_AT`/`__END_AT` là timestamp phục vụ join theo thời điểm; khoảng hiệu lực
là **[start,end)**. Mỗi product_id chỉ được có một thay đổi business khác biệt ở cùng updated_at.
Chỉ đổi unit_price/currency mới mở phiên bản mới; đổi updated_at nhưng business không đổi do AUTO CDC xử lý
theo track_history_column_list. Luồng hiện hành chặn price delete vì nó thường mang lại thời điểm của phiên bản trước.

Promo delete thường mang updated_at cũ của row nguồn, nên dùng log order. Không thay sequence promo
bằng updated_at hoặc thời gian ingest. Nguồn cũng phải bảo đảm một payload cho mỗi promo_id/LSN/seqval.
Code không che xung đột bằng dropDuplicates chỉ theo key/sequence; hai payload khác nhau được giữ để
điều tra. Xung đột vi phạm hợp đồng phải được xử lý trước khi chạy AUTO CDC, không dựa vào thứ tự file.

Không watermark theo updated_at: snapshot có hiệu lực từ 2015 và wave có thể đến sai thứ tự. Dedup
giữ state trong checkpoint để nhận replay lâu ngày. State của `dropDuplicates` tăng theo
số bản ghi phân biệt, vì vậy khi nghiệm thu ghi `numRowsTotal` và `memoryUsedBytes` từ `stateOperators`
của streaming progress, kèm kích thước checkpoint nếu có quyền đọc. Chưa đo trên dữ liệu thật.
Snapshot phục hồi sau khi vượt retention CDC (7 ngày theo cấu hình dự án) chỉ ghi bản đang tồn tại.
Snapshot không có tombstone, nên khuyến mãi đã xóa trong khoảng hở vẫn còn ở silver. Cần đối soát
khóa hiện hành và xử lý riêng trước khi xem trạng thái retail đã khớp.
Không reset checkpoint để kiểm idempotence. Thứ tự đến khác nhau phải cho cùng kết quả theo sequence.

## Chạy và bằng chứng trên dev

1. AD-DE-03 (#39) cung cấp rules; AD-DE-09 (#45 / PR #151) phải có bronze được nghiệm thu. Các task
   hạ tầng/policy/quyền cần hoàn tất như AD-DE-08. Không đổi namespace của bảng đã có chủ pipeline.
2. Seed wave 0, công bố snapshot/CDC bằng ADF, rồi chạy pipeline dev. Sau đó seed wave 1–3 và chạy ADF
   theo cửa sổ đã chốt, không seed đồng thời với ADF. Chỉ dùng CDC log mode cho flow này.
3. Khi có CLI và profile do người chạy chọn, từ `bundles/`:

```bash
databricks bundle validate --strict -t dev --profile <profile>
databricks bundle deploy -t dev --profile <profile>
databricks bundle run -t dev --profile <profile> instacart_batch
```

4. Chạy truy vấn [03 §2.4](03_silver_cdc_scd_dq.md#24-kiểm-tra-scd), đổi đúng namespace dev. Với sản phẩm
   thật thay giá ở wave 1 và 3, kiểm đủ ba giá/ba khoảng thời gian, chỉ một phiên bản đang mở. Kiểm toàn bảng
   không có product_id với hơn một __END_AT NULL, không overlap khoảng thời gian.
5. Chọn promo delete có log position mới nhất và chưa được insert lại; phải vắng ở silver. Không dùng
   lịch sử từng bị delete để kết luận một promo tái tạo sau đó phải vắng. So binary LSN/seqval để chọn sự kiện mới nhất.
6. Chạy lại pipeline cùng bronze, so số phiên bản/trạng thái promo trước-sau không đổi. Để kiểm đường ADF
   đọc lặp window, dùng nguồn dev thử có sự kiện trùng nhưng tên file công bố mới; giữ nguyên file cũ/checkpoint.
7. Thử dữ liệu xấu trong nguồn dev: price âm, currency sai, key/sequence/time lỗi, rescue, promo window sai;
   kiểm quarantine giữ payload gốc và đúng mọi lý do. Thử delete payload rỗng, op=3 và late update riêng.
8. Ghi pipeline/update ID, cửa sổ nguồn, product_id/promo_id chọn từ seed thật, giá/khoảng hiệu lực, số phiên
   bản trước-sau replay và event log lên issue #47. Expectations nằm trên valid views; task collect_dq/reconcile
   sẽ tích hợp ở task tương ứng, không tự ghi kết quả chưa chạy.

## Kiểm local và phần chưa nghiệm thu

```bash
uv run pytest tests/test_retail.py -q
uv run ruff check .
uv run ruff format --check .
```

Spark thật kiểm chuẩn hóa, DQ/quarantine, binary order, biểu thức sequence/delete, re-snapshot và native
Structured Streaming dedup qua micro-batch có sự kiện đến muộn/replay. Decorator/flow dp được giả lập
để kiểm wiring, thứ tự tạo target và rules. **Không** phải engine AUTO CDC: test không chứng minh
Delta SCD2 tạo/đóng phiên bản, tombstone retention, SCD1 delete hoặc transaction trên workspace.

Nghiệm thu dev còn chờ AD-DE-07 công bố Parquet và có hai bảng bronze CDC để chạy các bước
trên. Chưa chạy validate/deploy, seed/ADF wave 1–3 hoặc query lịch sử thật. PR giữ draft, issue #47 mở chờ
nghiệm thu G3; không dùng số dòng fixture để xác nhận ba phiên bản SCD2 trên cloud.

Tham khảo: [AUTO CDC](https://learn.microsoft.com/en-us/azure/databricks/ldp/cdc),
[Python API](https://learn.microsoft.com/en-us/azure/databricks/ldp/developer/ldp-python-ref-apply-changes).
