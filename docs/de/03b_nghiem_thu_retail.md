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

## Kết quả nghiệm thu dev (2026-11-18)

Bronze CDC thật từ AD-DE-07/AD-DE-09: snapshot wave 0 cùng các cửa sổ CDC wave 1–3.
Bronze có đủ `_lsn`/`_seqval` dạng `0x` + 20 hex và 0 dòng rescue.

- `bundle validate -t dev` đạt.
- Update `0608ff6b-ea92-4fd6-b5d2-6c5811c37ced` COMPLETED.
- Chạy lại cùng bronze, update `dc3621eb-caa4-422f-9cda-493593d5884d` COMPLETED, số liệu không đổi (bước 6).
- `product_prices_history`:
  - 54.656 dòng, bằng 49.688 snapshot cộng 4.968 thay đổi `_op=4`.
  - 49.688 phiên bản mở, không `product_id` nào có hơn một phiên bản mở, 0 khoảng chồng nhau.
  - 44.835 sản phẩm có 1 phiên bản, 4.738 có 2, 115 có 3.
  - Ví dụ `product_id=194`, các khoảng liền nhau:

    | Khoảng | Giá (USD) |
    |---|---|
    | 2015-01-01 → 2026-03-08 | 1.03 |
    | 2026-03-08 → 2026-03-15 | 1.08 |
    | 2026-03-15 → mở | 0.92 |

- `promotions`:
  - 3.810 dòng, bằng số `promo_id` có sự kiện cuối (theo LSN/seqval binary) khác delete.
  - 0 promo có sự kiện cuối là delete còn ở silver (bước 5).
- Hai bảng quarantine: 0 dòng.

**Chưa nghiệm thu:**
- Chưa đo state `dropDuplicates` (`numRowsTotal`, `memoryUsedBytes`).
- Chưa thử dữ liệu xấu ở bước 7.
- Chưa thử file công bố lặp ở bước 6.

Tham khảo: [AUTO CDC](https://learn.microsoft.com/en-us/azure/databricks/ldp/cdc),
[Python API](https://learn.microsoft.com/en-us/azure/databricks/ldp/developer/ldp-python-ref-apply-changes).
