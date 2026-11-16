# 01 · Dữ liệu thực nghiệm: nhận gì từ DE, khám phá, chọn ngưỡng

**Đầu vào:** checklist [de/08 §7](../de/08_du_lieu_cho_ds.md) đã đạt (cổng G3 của DS).

**Đầu ra:**
- Notebook `notebooks/ds/01_eda.py` có số liệu khám phá dữ liệu.
- Lưới MinSupport cho từng cấp item, chốt vào `src/ml/experiments.yml`.
- Hàm `src/ml/data.py` đọc dữ liệu theo tham số thực nghiệm.

**Xong khi:** biết, với từng cấp item và từng MinSupport, có **bao nhiêu item phổ biến** và **bao nhiêu cặp ứng viên**. Từ đó ước được thời gian chạy trước khi bật lưới lớn.

---

## 1. Hợp đồng dữ liệu với DE

DE tạo các bảng dưới đây **một lần cho mỗi `data_version`** ([de/08](../de/08_du_lieu_cho_ds.md)). DS chỉ đọc, không sửa. Muốn đổi dữ liệu thì DE tạo `v2`, DS chạy lại thực nghiệm với `data_version = 'v2'`.

| Bảng | Hạt | Cột chính | Dùng cho |
|---|---|---|---|
| `ds_data.baskets` | phiên bản × đơn | `data_version`, `order_id`, `user_id`, `split`, `items array<int>` (product_id), `n_items` | Bản cố định của `silver.baskets` |
| `ds_data.item_index` | grain × item | `data_version`, `grain`, `item_key`, `idx`, `item_name`, `freq_prior` | Đổi `idx` ↔ tên khi đọc luật |
| `ds_data.transactions` | grain × đơn | `data_version`, `grain`, `order_id`, `split`, `items array<int>` (idx, tăng dần, không trùng), `n_items`, `scale_bucket` (0–99) | Đầu vào của mọi thuật toán |
| `ds_data.holdout_eval` | đơn holdout | `data_version`, `order_id`, `eval_part` (`val`/`test`), `context array<int>` (nửa đầu theo thứ tự bỏ vào giỏ), `targets array<int>` (phần còn lại), cấp product | E7: đánh giá gợi ý |
| Volume `ds_data.exports` | file | `v1/<grain>/scale=<pct>/part-*.txt`, kèm `v1/_manifest.json` (số giao dịch theo cấp × quy mô) | Hadoop local ([03](03_hadoop_mapreduce_local.md)) |

**Ba cấp item (`grain`):**

| `grain` | Số item | Số giỏ `prior` | Giống dữ liệu nào trong bài báo |
|---|---|---|---|
| `department` | 21 | 3.214.874 | Ít item, giao dịch dày, giống Poker (78 item) |
| `aisle` | 134 | 3.214.874 | Giống Otto (93 item) |
| `product` | 49.677 có xuất hiện trong prior (danh mục 49.688) | 3.214.874 | **Thưa, nhiều item.** Bài báo chưa thử trường hợp này |

Số giỏ `prior` = 3.421.083 đơn − 131.209 đơn train − 75.000 đơn test. Con số chính xác lấy từ checklist G3 ([de/03 §9](../de/03_silver_cdc_scd_dq.md)).

**Quy mô (`scale_pct`)** lọc theo `scale_bucket < scale_pct`:
- `scale_bucket = pmod(xxhash64(order_id), 100)`, nên tập 10% nằm trọn trong tập 25%, tập 25% nằm trọn trong 50%, cứ thế đến 100%. Chạy lại luôn ra **đúng cùng tập giỏ**.
- 200% là 100% **nhân đôi** (mỗi giỏ xuất hiện hai lần). Cách này giữ nguyên tỉ lệ support, chỉ tăng khối lượng dữ liệu. Đây là cách làm sizeup quen thuộc trong các bài đo hiệu năng.

## 2. Đọc dữ liệu: `src/ml/data.py`

```python
# src/ml/data.py
"""Đọc giao dịch theo tham số thực nghiệm. Dùng chung cho DApriori, DATID, FP-Growth."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TX = "instacart.ds_data.transactions"


def load_transactions(spark: SparkSession, *, grain: str, scale_pct: int, top_n: int | None,
                      data_version: str = "v1", split: str = "prior") -> DataFrame:
    """Trả DataFrame (order_id, items array<int>) đã lọc theo grain, quy mô và top-N item.

    - scale_pct 10/25/50/100 lọc theo scale_bucket (tập con lồng nhau, tất định).
    - scale_pct 200 = 100% nhân đôi (giữ nguyên support, tăng khối lượng).
    - top_n: chỉ giữ item có idx < top_n (idx xếp theo tần suất giảm dần), bỏ giỏ rỗng.
    """
    df = (spark.table(TX)
          .where((F.col("data_version") == data_version) & (F.col("grain") == grain) & (F.col("split") == split))
          .where(F.col("scale_bucket") < min(scale_pct, 100))
          .select("order_id", "items"))
    if top_n is not None:
        df = (df.withColumn("items", F.filter("items", lambda i: i < F.lit(top_n)))
                .where(F.size("items") > 0))
    if scale_pct == 200:
        df = df.unionByName(df.withColumn("order_id", -F.col("order_id")))   # bản sao, id âm để không trùng
    return df


def to_rdd(df: DataFrame):
    """RDD[tuple[int, ...]] cho DApriori/DATID. items đã sort tăng dần từ DE."""
    return df.select("items").rdd.map(lambda r: tuple(r[0]))
```

**Không `cache()` trong `load_transactions`.** Mỗi thuật toán tự quyết định cache cái gì: DApriori cache RDD gốc, DATID cache RDD đã thu gọn. Nếu cache sẵn ở đây thì phép đo bộ nhớ sẽ không còn phân biệt được hai thuật toán.

## 3. Khám phá dữ liệu (EDA)

Chạy một lần, trên toàn bộ `v1`, rồi ghi kết quả vào báo cáo. Các con số trong cột "Kỳ vọng" là ước lượng từ bộ dữ liệu gốc, **phải đo lại**.

```sql
-- 3.1 Kích thước giỏ theo cấp
SELECT grain, count(*) AS n_tx, round(avg(n_items), 2) AS avg_len,
       percentile(n_items, 0.5) AS p50, percentile(n_items, 0.95) AS p95, max(n_items) AS max_len
FROM instacart.ds_data.transactions
WHERE data_version = 'v1' AND split = 'prior'
GROUP BY grain;
```

| Chỉ số | Kỳ vọng (product) | Ý nghĩa cho thuật toán |
|---|---|---|
| `avg_len` | khoảng 10 | Số cặp trung bình mỗi giỏ là C(10,2) = 45 |
| `max_len` | khoảng 145 | Một giỏ dài sinh C(145,3) ≈ 500 nghìn bộ ba. Đây là lý do phải lọc giỏ theo item ứng viên trước khi sinh tổ hợp ([02 §2](02_thuat_toan_spark.md)) |
| `p95` | khoảng 25–30 | Đuôi phân phối quyết định thời gian của pha k = 3 |

```sql
-- 3.2 Số item phổ biến theo MinSupport (đường cong quyết định lưới tham số)
WITH n AS (SELECT grain, count(*) AS n_tx FROM instacart.ds_data.transactions
           WHERE data_version = 'v1' AND split = 'prior' GROUP BY grain),
     f AS (SELECT grain, idx, freq_prior FROM instacart.ds_data.item_index WHERE data_version = 'v1')
SELECT f.grain, s.min_support,
       count_if(f.freq_prior >= ceil(s.min_support * n.n_tx)) AS n_frequent_items
FROM f JOIN n USING (grain)
CROSS JOIN (SELECT explode(array(0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2)) AS min_support) s
GROUP BY f.grain, s.min_support
ORDER BY f.grain, s.min_support;
```

Từ `n_frequent_items` (số phần tử của L₁) suy ra số ứng viên bậc 2: n(C₂) = n(L₁)·(n(L₁)−1)/2. Đây là kích thước hash set được broadcast ở pha 2 của DApriori:

| n(L₁) | n(C₂) | Broadcast (ước lượng, tuple Python) |
|---|---|---|
| 100 | 4.950 | Không đáng kể |
| 1.000 | 499.500 | Khoảng 50–80 MB |
| 3.000 | 4,5 triệu | **Vài trăm MB, sát giới hạn** của driver 14 GB khi cộng thêm bản sao trong worker Python |

Kết luận cần chốt ở bước này: **MinSupport nhỏ nhất cho cấp product** phải giữ n(L₁) khoảng ≤ 2.000. Theo đường cong của bộ Instacart gốc, giá trị này rơi vào khoảng 0,001. Ghi số đo thật vào báo cáo.

```sql
-- 3.3 Phân phối tần suất item (đuôi dài) để vẽ biểu đồ log-log trong báo cáo
SELECT grain, idx, freq_prior FROM instacart.ds_data.item_index
WHERE data_version = 'v1' ORDER BY grain, idx;
```

```sql
-- 3.4 Holdout: bao nhiêu đơn đánh giá được
SELECT count(*) AS n_eval, round(avg(size(context)), 2) AS avg_context
FROM instacart.ds_data.holdout_eval WHERE data_version = 'v1';
-- Chỉ đơn có ≥ 2 món mới vào được holdout_eval (cần ít nhất 1 món làm ngữ cảnh + 1 món để đoán).
-- eval_part chia đôi theo hash(order_id): 'val' để chọn cấu hình champion, 'test' để báo cáo số cuối (04 §7).
```

## 4. Lưới MinSupport theo cấp item

Bản đầu của hình kiến trúc đề xuất `minSupport ∈ {0.001, 0.002, 0.005, 0.01}` và `minConfidence ∈ {0.1, 0.2, 0.3}`. Lưới này hợp với cấp **product**. Hai cấp còn lại có ít item hơn nhiều nên support cao hơn hẳn; dùng lưới trên thì DApriori exhaustive sẽ bùng nổ tổ hợp. Mỗi cấp cần lưới riêng:

| `grain` | `min_support` | `max_k` | `min_confidence` | Lý do |
|---|---|---|---|---|
| `product` | 0.01, 0.005, 0.002, 0.001 | 3 | 0.1, 0.2, 0.3 | Như đề xuất ban đầu. k ≤ 3 vì giỏ dài và có 49 nghìn item |
| `aisle` | 0.1, 0.05, 0.02, 0.01 | không giới hạn | 0.3, 0.5, 0.7 | Tái hiện điều kiện bài báo (exhaustive) |
| `department` | 0.2, 0.1, 0.05 | không giới hạn | 0.5, 0.7, 0.9 | Ít item: tập phổ biến dài, kiểm tra được giới hạn hàm mũ |

Bước EDA được phép **sửa lưới này** nếu đường cong ở mục 3.2 cho thấy một ngưỡng quá nặng (ví dụ n(L₁) > 3.000) hoặc quá nhẹ (ví dụ n(L₂) = 0). Mọi thay đổi ghi vào `experiments.yml` kèm một dòng lý do trong commit.

## 5. Lỗi giữa chừng thì chạy lại thế nào

| Tình huống | Cách xử lý | Vì sao an toàn |
|---|---|---|
| Đọc `ds_data` báo bảng không tồn tại | Chờ DE chạy xong `ds-prepare` (checklist de/08) | DS không tự tạo bảng `ds_data` |
| Số liệu EDA khác hẳn kỳ vọng | Đối chiếu với `ds_data._manifest` và checklist G3; báo DE nếu lệch | DS chỉ đọc, chưa ghi gì phụ thuộc vào số liệu sai |
| DE tạo `v2` giữa lúc DS đang chạy lưới | Không ảnh hưởng: mọi truy vấn đều lọc theo `data_version` | `v1` không bao giờ bị ghi đè ([de/08 §2](../de/08_du_lieu_cho_ds.md)) |
