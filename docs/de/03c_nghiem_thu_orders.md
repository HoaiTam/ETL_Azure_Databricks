# Nghiệm thu silver đơn hàng — AD-DE-12

`silver_instacart.py` nằm trong glob batch của AD-DE-08/09, không thêm pipeline/job/cluster.
Nguồn là bronze đã nghiệm thu. Tên catalog/schema lấy từ configuration, có quote identifier;
dev vẫn dùng `<user>_silver`, pipeline đặt session UTC.

| Dataset | Cách xử lý |
|---|---|
| order_headers_assessed | View batch: chuẩn hóa bằng try_cast, đánh giá DQ và ghi metrics warn cho mọi bản nguồn |
| order_headers_canonical | View batch: chọn bản hợp lệ/ingest mới nhất mỗi order_id; fail nếu đơn không có bản hợp lệ |
| silver.order_time_sim | Materialized view, giữ header, dùng chế độ canonical của order_time_sim_sql; fail khi order_ts_sim NULL |
| silver.orders_batch | Materialized view prior/test từ clock, source=batch, is_test, cluster_by user_id |
| order_items_prior_valid / order_items_prior_src | View stream: ITEM_INPUT_RULES rồi chọn cột nguồn AUTO CDC |
| silver.order_items_prior | AUTO CDC SCD1 theo order_id/product_id, sequence struct(_ingest_ts, _file_mtime, _source_file), cluster_by order_id |
| silver.quarantine_orders / quarantine_order_items_prior | Streaming table giữ mọi raw payload lỗi và tên luật lỗi |

## Clock và DQ header

Clock đọc **đủ prior/train/test** để biết tổng khoảng cách từ đơn hiện tại tới đơn cuối. Không lọc
train trước khi tính, không dùng giờ máy hoặc tự viết lại công thức. Truy vấn helper AD-DE-02 vẫn
trả TIMESTAMP_LTZ với UTC tường minh; kiểm epoch khi so Python để không nhầm datetime local khi collect.

`ORDER_INPUT_RULES` tái sử dụng ORDER_RULES, thêm khóa dương, split hợp lệ, order_number, khả năng parse
gap, gap không rỗng sau đơn đầu và ingest time hợp lệ. `ITEM_INPUT_RULES` tái sử dụng ITEM_RULES,
thêm khóa dương và ingest time vì AUTO CDC cần sequence khác NULL. Không sửa dictionary chung AD-DE-03.
Try_cast giúp kiểu sai được ghi quarantine dưới ANSI; raw payload/metadata được giữ trước chuẩn hóa.

Header có bản lỗi và bản tốt: ưu tiên bản đạt DQ, rồi _ingest_ts mới nhất. Hash raw làm tie-break ổn định;
không coi hash là quy tắc giải quyết xung đột business. Nguồn Instacart phải có header nhất quán và trọn
lịch sử mỗi khách; nếu có payload hợp lệ mâu thuẫn cùng key/thời điểm phải xử lý nguồn trước nghiệm thu.
Nếu mọi bản của một order đều lỗi, giữ một đại diện để expectation fail chặn clock. Không drop hẳn header
rồi tự tính thời gian như lịch sử vẫn đầy đủ. Quarantine giữ **tất cả** bản lỗi, kể cả bản không được chọn.

Metrics trên assessed view là toàn bộ snapshot header; canonical gate là sau chọn phiên bản.
Quarantine đọc stream bronze, chỉ thêm bản lỗi mới theo checkpoint. Không đồng nhất ba population này
hoặc cộng số liệu DQ giữa các view như cùng một tập. LDP fail/rollback theo flow, không tự khẳng định
mọi bảng khác cùng rollback; kiểm event log và quarantine khi clock fail.

## Batch và món prior

orders_batch chỉ giữ prior/test sau khi có clock đủ split, không tạo header train. Giữ _ingest_ts phục vụ
lineage. Không công bố _rescued_data (sau DQ luôn NULL), _raw/_dq_failed/_source_file/_file_mtime trong MV này.
is_test chỉ đúng cho test; đơn test không có món ở nguồn, không tự sinh món để đi vào baskets.

Món hàng chỉ đọc bronze.order_products_prior; không đọc train. Đầu vào stream qua DQ rồi AUTO CDC SCD1,
key kép order_id/product_id, sequence là `struct(_ingest_ts, _file_mtime, _source_file)` để hai bản cùng
micro-batch vẫn có thứ tự tất định. File replay được bronze đọc lại ở
đường dẫn mới có thể tạo event mới; upsert target là trách nhiệm của engine AUTO CDC, không append thẳng
và không thay bằng dropDuplicates giữ key mãi để vô tình loại các correction hợp lệ. File gốc không bị sửa.
_ingest_ts giữ trong target, các metadata file/rescue không đưa vào target; input cần một payload nhất quán
cho mỗi key/sequence. FK/liên kết đầy đủ với header/danh mục sẽ được đối soát thêm ở AD-DE-17.

## Chạy dev và đối soát

1. Nghiệm thu AD-DE-09 (#45/PR #151), quyền UC/policy/schema của AD-DE-08; AD-DE-02/03 cung cấp helper/rules.
   Các nguồn CDC/OFF khác của pipeline cũng phải sẵn sàng. Không deploy prod thủ công.
2. Khi có CLI và profile đã chọn, chạy trong bundles:

```bash
databricks bundle validate --strict -t dev --profile <profile>
databricks bundle deploy -t dev --profile <profile>
databricks bundle run -t dev --profile <profile> instacart_batch
```

3. Bộ đầy đủ cần kiểm các giá trị sau trên namespace dev đã resolve, sau khi xử lý hết DQ:

| Kiểm tra | Kỳ vọng nguồn đầy đủ |
|---|---|
| order_time_sim count và distinct order_id | 3.421.083, bằng distinct header bronze |
| order_time_sim order_ts_sim NULL | 0 |
| orders_batch count và distinct order_id | 3.289.874 = 3.421.083 − 131.209, kiểm lại với nguồn thật |
| orders_batch eval_set/source/is_test | chỉ prior/test, batch, is_test đúng cho test |
| order_items_prior count và distinct key kép | cùng bằng 32.434.489 |
| Batch đọc món train | không có đường đọc train vào target prior |

4. So order_time_sim với Python order_ts_sim trên lịch sử đầy đủ một số khách, gồm train/test, đơn >9,
   gap NULL đầu tiên và replay. Với mẫu 1%, so manifest của mẫu; không áp số toàn bộ lên mẫu/fixture.
5. Chạy lại cùng pipeline/bronze không thay đổi: số header/key kép và giá trị clock không đổi. Với replay
   có chủ đích ở dev, công bố file copy tên mới qua staging/landing, giữ nguyên file gốc/checkpoint;
   sau update, target prior vẫn có đúng một dòng mỗi key kép. Test correction cần ingest time mới hơn.
6. Thử dữ liệu xấu ở nguồn dev, xem quarantine và event log. Lỗi header không có bản phục hồi phải chặn
   clock; một bản lỗi cạnh bản hợp lệ không được làm thay thời gian của bản tốt. Kiểm raw và mọi tên luật.

## Đo MERGE đầu tiên

Chưa có số đo thực tế trong phiên này. Khi chạy full load lần đầu trên workspace, ghi lại:

- Pipeline/update ID, target/flow order_items_prior_scd1, ngày chạy và commit nguồn.
- VM/policy, channel/runtime thực tế, số worker, số dòng input/output và quy mô sample/full.
- Thời điểm bắt đầu/kết thúc flow và elapsed giây từ UI/event log; ghi riêng tổng thời gian update,
  thời gian khởi động cluster và thời gian flow để không gộp chúng thành thời gian MERGE.
- Kết quả count/unique key và DQ sau run; link bằng chứng vào issue #48, chuyển cho AD-DE-26/benchmark.

Không full refresh hoặc xóa target/checkpoint để cố tạo lại phép đo khi nguồn/bảng đang được dùng.
Không thay thời gian pytest/Spark local hoặc ước lượng trong tài liệu thành kết quả MERGE 32 triệu dòng.

## Kiểm local và giới hạn

`tests/test_instacart.py` dùng Spark thật cho chuẩn hóa/quarantine, canonical header, SQL clock và so với
Python trên đủ ba split, count/flag batch. Native stream hai micro-batch kiểm input hợp lệ và event replay
vẫn đi tiếp với ingest time mới. Decorator/flow dp được giả lập để kiểm wiring/rules/key/sequence/clustering.
Nó **không** chứng minh AUTO CDC upsert target, transaction Delta hoặc metrics Lakeflow trên workspace.

```bash
uv run pytest tests/test_instacart.py tests/test_sim_time.py -q
uv run ruff check .
uv run ruff format --check .
```

## Kết quả nghiệm thu dev (2026-11-19)

Bộ Instacart thật trên bronze AD-DE-09.
- `bundle validate -t dev` đạt.
- Update [`c1292e3c`](https://adb-7405615740735217.17.azuredatabricks.net/#joblist/pipelines/4df2ff96-be27-4af6-8e87-e2e876da866e/updates/c1292e3c-2a52-4458-9752-04cea064b3c7)
  COMPLETED.
- Sau khi gộp main có silver retail, update `b3865e15-e637-41ba-8e52-30223d9c6590` COMPLETED.

| Kiểm tra | Kết quả |
|---|---:|
| `order_items_prior`: `count(*)` / `count(DISTINCT order_id, product_id)` | 32.434.489 / 32.434.489 |
| `order_time_sim`: số dòng / `order_ts_sim IS NULL` | 3.421.083 / 0 |
| `orders_batch`: số dòng / số `order_id` khác nhau (prior + test, không train) | 3.289.874 / 3.289.874 |
| `quarantine_orders` / `quarantine_order_items_prior` | 0 / 0 |

**Chưa nghiệm thu:**
- Replay AUTO CDC bằng file công bố lại.
- Đo thời gian MERGE đầu tiên vào `ops.benchmark_results`.
- Đối soát FK với danh mục (AD-DE-17).
