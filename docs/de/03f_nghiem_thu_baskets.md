# Nghiệm thu view hợp nhất và baskets — AD-DE-15

## Phạm vi và bootstrap

- [`create_views.sql`](../../src/tasks/create_views.sql) tạo hai **view UC**, không phải bảng LDP:
  `silver.orders`, `silver.order_items`. Batch/stream vẫn sở hữu target riêng.
- MV `silver.baskets` của pipeline batch ở cuối
  [`silver_instacart.py`](../../src/pipelines/batch/silver_instacart.py), helper
  [`common/baskets.py`](../../src/common/baskets.py) đọc trực tiếp bốn bảng silver, không đọc view.
- AD-DE-12 (#48) đã hoàn tất. Bật giỏ và nghiệm thu dữ liệu thật sau khi AD-ST-05 (#68)
  có đủ hai bảng stream đúng schema.

Lần bootstrap: chạy batch với `enable_baskets=false` (mặc định) để tạo nguồn batch.
Sau khi triển khai và chạy AD-ST-05, bật `enable_baskets` cho lần deploy dev tiếp theo.
Giữ cùng catalog và schema silver đã resolve giữa hai pipeline. Hai target header/items stream là hai
flow riêng. Header đến trước món được đếm cảnh báo và tạm bỏ khỏi MV; chỉ prior rỗng làm fail.
Giỏ đang chờ sẽ xuất hiện ở lần chạy sau khi món đã commit.

## Chạy dev và hai view

Trong `bundles/`, kiểm chế độ mặc định trước khi có hai bảng stream:

```bash
databricks bundle validate --strict -t dev --profile <profile>
databricks bundle deploy -t dev --profile <profile>
databricks bundle run -t dev --profile <profile> instacart_batch
```

Update ở chế độ mặc định phải thành công và schema silver chưa có `baskets`. Khi hai bảng stream
đã sẵn sàng, deploy dev với `BUNDLE_VAR_enable_baskets=true`, chạy lại batch và chuẩn bị đối soát.

Chạy SQL file trên warehouse **serverless** `wh-instacart-bi` bằng danh tính có quyền đọc cả bốn bảng
nguồn, tạo/thay view trong schema dev theo quyền của AD-OPS-20. Điền hai named parameters STRING:

| Tham số | Ví dụ dev | Ý nghĩa |
|---|---|---|
| `catalog` | `instacart` | Tên riêng catalog, không bọc backtick |
| `silver_schema` | `p2_silver` | Schema đã resolve của cả batch và stream, không bọc backtick |

File chọn namespace bằng `USE IDENTIFIER(...)`, rồi tạo hai view. Chạy **toàn bộ ba câu trong cùng phiên**,
dừng nếu USE lỗi; không chọn riêng các câu CREATE với schema đang mở trong editor.
Không có giá trị prod mặc định. Task AD-DE-18 sẽ truyền catalog/schema tương ứng bundle vào SQL file;
PR này chưa tạo job điều phối hay warehouse mới.

Chạy cả file lần nữa với cùng tham số, phải thành công và không đổi dữ liệu. View dùng UNION ALL và
phản ánh target hiện tại, không cần chạy lại DDL mỗi khi có stream mới. SQL đổi namespace của phiên;
nếu tiếp tục chạy lệnh khác thì chọn rõ namespace phù hợp.

## Đối soát thật

Đọc trong notebook nghiệm thu bằng tên đầy đủ đã resolve, giữ nguồn đứng yên trong lúc đo.
Các action dưới đây không thuộc hàm dataset LDP.

```python
from pyspark.sql import functions as F

namespace = "instacart.p2_silver"  # thay đúng catalog/schema dev
orders = spark.read.table(f"{namespace}.orders")
items = spark.read.table(f"{namespace}.order_items")
batch = spark.read.table(f"{namespace}.orders_batch")
stream = spark.read.table(f"{namespace}.stream_orders")
prior_items = spark.read.table(f"{namespace}.order_items_prior")
stream_items = spark.read.table(f"{namespace}.stream_order_items")
baskets = spark.read.table(f"{namespace}.baskets")

assert orders.count() == batch.count() + stream.count()
assert orders.count() == orders.select("order_id").distinct().count()
assert items.count() == prior_items.count() + stream_items.count()
assert items.count() == items.select("order_id", "product_id").distinct().count()
assert batch.join(stream, "order_id", "inner").count() == 0
assert prior_items.join(batch.where("eval_set = 'prior'"), "order_id", "left_anti").count() == 0
assert stream_items.join(stream, "order_id", "left_anti").count() == 0
assert baskets.count() == baskets.select("order_id").distinct().count()
assert baskets.where("split NOT IN ('prior', 'holdout') OR n_items < 1").count() == 0
assert baskets.where("n_items <> size(items) OR NOT _items_valid OR NOT _source_disjoint OR dup_cnt <> 1").count() == 0
assert baskets.join(batch.where("eval_set = 'test'"), "order_id", "inner").count() == 0
baskets.groupBy("split").count().show(truncate=False)
orders.groupBy("source", "eval_set", "is_test").count().show(truncate=False)
```

Khi **full release và đã phát đủ train**, đối chiếu số thật:

| Dataset | Mục tiêu đối soát |
|---|---|
| orders | 3.421.083 header, gồm 75.000 test |
| order_items | 33.819.106 khóa (32.434.489 prior + 1.384.617 stream train) |
| baskets prior | 3.214.874 giỏ |
| baskets holdout | 131.209 giỏ |

Nếu stream rỗng, chỉ có prior là trạng thái tạm hợp lệ, **chưa** đạt G3/đầu vào DS.
`items ARRAY<INT>` là tập mã sản phẩm tăng dần, n_items là kích thước tập, không phải số lượng mua.
Snapshot AD-DE-20 cần kiểm miền số rồi cast tường minh sang schema INT của ds_data, không đổi mã khóa.

## Replay và lỗi

1. Giữ nguyên nguồn, chạy lại SQL và refresh batch. So basket snapshot trước/sau bằng exceptAll hai chiều,
   số giỏ/sản phẩm trong giỏ phải không đổi. Không full refresh bronze hay reset checkpoint.
2. Sau AD-ST-07 phát lại train với run-id khác, đợi cả hai target SCD1 hoàn tất, refresh batch rồi đối soát
   lại số header/items/giỏ và nội dung giỏ. Local test không chứng minh engine AUTO CDC/Event Hubs đã replay thành công.
3. Nếu non_empty_prior_basket fail: tìm header prior chưa có món; nếu unique_basket_order/disjoint_sources fail:
   kiểm giao mã đơn prior/test với stream hoặc cùng order_id nhưng user_id khác. Xem dữ liệu nguồn và event log;
   không xóa nguồn hoặc đổi split để vượt cổng. Orphan items cần đối soát riêng ở AD-DE-17.
4. Ghi issue #51: commit, catalog/schema, ngày chạy, pipeline/update ID batch/stream, trạng thái update,
   số dòng view/baskets theo split và distinct key, lần chạy SQL thứ hai, kết quả replay, event log constraints.
   MV có thể còn phiên bản thành công cũ sau update lỗi; kiểm update ID, không chỉ nhìn bảng còn tồn tại.

## Trạng thái hiện tại

Đã kiểm Spark 3.5 local, chạy nguyên SQL file có parameters hai lần và kiểm các biến đổi baskets bằng
fixture prior/train/test. `dp` trong test kiểm wiring/điều kiện, không thay engine LDP/UC.

Sau khi rebase lên main có AD-DE-10, strict bundle validation và deploy pipeline dev đã đạt.
Bundle giữ `enable_off`, `off_root` và `enable_baskets`. Lượt chạy với `enable_baskets=false`
kết thúc `COMPLETED`. Schema silver chưa có `baskets` trước và sau lượt chạy.
[Update dev](https://adb-7405615740735217.17.azuredatabricks.net/#joblist/pipelines/4df2ff96-be27-4af6-8e87-e2e876da866e/updates/89561b5f-9702-40ec-8720-4d4a0fa6d5a7).

## Kết quả nghiệm thu dev

Nghiệm thu ngày 25/11/2026 từ commit `f1d600b` sau khi AD-ST-05 đã merge. Pipeline dev cách ly
dùng nguyên helper cùng định nghĩa MV của batch trong schema `23521394_ad15_silver` của catalog `instacart`.
Nguồn prior/test lấy từ Silver batch dev. Nguồn train đầy đủ lấy món từ Bronze và header từ
`order_time_sim`, ánh xạ sang schema Silver stream vì lượt phát đủ của AD-ST-07 còn mở.
[Bằng chứng replay 1.000 đơn của AD-ST-05](evidence/ad-st-05/2026-11-22-silver-replay.md) ·
[AD-ST-07](https://github.com/Vinh5905/ETL_Azure_Databricks/issues/70).

Chạy nguyên `create_views.sql` hai lần trên `wh-instacart-bi`. Mỗi lượt bọc trong
`BEGIN … END` để `USE IDENTIFIER` và cả hai câu tạo view chạy trong cùng phiên.
Hai statement `01f1e898-6728-12f1-a3b3-1c8d2e593458` và
`01f1e898-6a79-1ba8-82d8-6700648b9229` đều báo `SUCCEEDED`.

| View | Nhóm nguồn | Số dòng | Khóa phân biệt |
|---|---|---:|---:|
| `orders` | Batch prior | 3.214.874 | 3.214.874 |
| `orders` | Batch test | 75.000 | 75.000 |
| `orders` | Train qua schema stream | 131.209 | 131.209 |
| `order_items` | Batch prior | 32.434.489 | 32.434.489 |
| `order_items` | Train qua schema stream | 1.384.617 | 1.384.617 |

| Split của `baskets` | Số giỏ | Khóa phân biệt | Tổng `n_items` | Giỏ sai hợp đồng |
|---|---:|---:|---:|---:|
| prior | 3.214.874 | 3.214.874 | 32.434.489 | 0 |
| holdout | 131.209 | 131.209 | 1.384.617 | 0 |

Update Lakeflow hoàn tất với các expectations đang bật. Truy vấn đối soát xác nhận 0 giỏ test,
0 khóa đơn giao nhau giữa batch và stream, 0 món mồ côi và 0 khóa giỏ trùng.
[Update tạo baskets](https://adb-7405615740735217.17.azuredatabricks.net/#joblist/pipelines/c01b131c-6cf8-48dc-8d17-ffe4d6e9bc4f/updates/d3bd82c8-1d85-4ad3-a398-cd67b7a1651f).

Refresh thường lần hai trên cùng nguồn cho kết quả `EXCEPT ALL` bằng 0 theo cả hai chiều
trên năm cột nghiệp vụ, so với bản chụp 3.346.083 giỏ của lượt đầu. Statement đối chiếu
`01f1e89a-1890-1f8e-80a0-d401ce8159d6` báo `SUCCEEDED`.
[Update lần hai](https://adb-7405615740735217.17.azuredatabricks.net/#joblist/pipelines/c01b131c-6cf8-48dc-8d17-ffe4d6e9bc4f/updates/efb1fbe4-f727-47aa-a9a2-0225523b766e).
