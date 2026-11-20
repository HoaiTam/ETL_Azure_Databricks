# 05 · Tầng gold: bảng phục vụ BI, tối ưu lưu trữ, benchmark

**Đầu vào:**
- Silver đạt G3 ([03](03_silver_cdc_scd_dq.md)).
- SQL warehouse `wh-instacart-bi` hoặc lớp serving dự phòng ([devops/06 §4](../devops/06_de_den_dashboard.md)).

**Đầu ra:**
- Các materialized view trong `gold` mà Power BI đọc.
- Bảng `ops.benchmark_results` (rubric 3.1, 3.4).

**Xong khi:**
- Mọi bảng gold Power BI dùng đều nhỏ (≤ vài triệu dòng), đã tổng hợp sẵn và có kiểm tra tổng khớp với silver.
- Benchmark có đủ 4 định dạng.

---

## 1. Nguyên tắc thiết kế gold

1. **Gold phục vụ câu hỏi, không phục vụ bảng nguồn.** Mỗi bảng gắn với một trang hoặc visual trong Power BI ([07](07_power_bi.md)).
2. **Tổng hợp sẵn.** Power BI không quét 33 triệu dòng mỗi lần mở trang. Bảng lớn nhất Power BI đọc là `department_sales` (cỡ trăm nghìn dòng).
3. **Tính lại được hoàn toàn từ silver.** Gold là materialized view, nên chạy lại bao nhiêu lần cũng ra cùng kết quả.
4. **Một bảng fact chi tiết làm nền.** `fact_order_lines` (đã gắn giá theo thời điểm) được tính một lần; các bảng tổng hợp đọc từ đó, không lặp lại phép join đắt.

| Bảng gold | Hạt (grain) | Dùng ở | Cỡ ước tính |
|---|---|---|---|
| `fact_order_lines` | 1 dòng / món trong đơn | Nền cho bảng khác (Power BI **không** đọc) | 33,8 triệu |
| `kpi_daily` | ngày × nguồn | Trang 1 | Vài nghìn |
| `kpi_by_dow_hour` | thứ × giờ | Trang 1, 3 | 168 |
| `department_sales` | ngày × aisle | Trang 4, RLS | Khoảng vài trăm nghìn |
| `product_360` | sản phẩm | Trang 2, dim sản phẩm | 49.688 |
| `product_pair_counts` | cặp sản phẩm (top) | Trang 2 | ≤ 200 nghìn |
| `dim_date`, `dim_department` | ngày / ngành | Dim chung | Nhỏ |
| `bi_user_access` | email × department | RLS | Nhỏ |
| `stream_kpi_1m`, `stream_pair_counts_10m` | phút / cửa sổ 10 phút | Trang 5, tile real-time | Nhỏ ([04](04_streaming.md)) |
| View DS: `association_rules`, `recommendations`, `experiment_metrics`, `holdout_metrics` | luật / cặp gợi ý / tổ hợp thực nghiệm | Trang 2, 6, 7 | Nhỏ. **View**, do job DS tạo ([ds/05 §4](../ds/05_mlflow_serving_bao_cao.md)), không thuộc pipeline `instacart_batch` |

## 2. Code

### 2.1 Fact có giá theo thời điểm (point-in-time join)

[`gold.py`](../../src/pipelines/batch/gold.py) khai báo MV `gold.fact_order_lines` trong
pipeline batch, phân cụm theo `order_date, department_id`.
[`build_fact_order_lines`](../../src/common/gold.py) đọc hai view hợp nhất
`silver.orders` và `silver.order_items`, khớp header theo `order_id, source` rồi lấy ngành hàng
và aisle từ `silver.dim_product`.

- Giá lấy từ `silver.product_prices_history` theo khoảng nửa mở
  `__START_AT <= order_ts_sim < __END_AT`. `__END_AT IS NULL` là phiên bản còn hiệu lực.
- Từ các ngày có header, gộp khuyến mãi SCD1 theo `product_id, order_date` và lấy mức giảm cao
  nhất trong các khoảng bao gồm ngày đó. Mỗi dòng fact vì vậy nhận tối đa một mức giảm.
- `revenue_sim` là doanh thu giả lập, tính bằng giá theo thời điểm nhân với phần trăm còn lại
  sau khi giảm, làm tròn 2 chữ số. Khi không có khuyến mãi, `discount_pct` là NULL và doanh thu
  lấy bằng giá. Lịch sử giá cố định theo SCD2, còn khuyến mãi dùng trạng thái SCD1 hiện có.
- Giữ cả món thiếu dữ liệu tham chiếu để kiểm lỗi. Thiếu header hoặc danh mục làm flow lỗi.
  Giá NULL được ghi vào metrics expectation `priced` và làm check G4 thất bại.

**Bật sau khi Silver sẵn sàng.** Mặc định `enable_gold_fact=false` để các lượt bootstrap batch
có thể chạy trước stream. Sau khi có hai view hợp nhất và giá Silver, chỉ dùng
`BUNDLE_VAR_enable_gold_fact=true` để validate và deploy target dev, rồi refresh
`instacart_batch` để tạo fact. Khi G4 sẵn sàng, mở PR đặt `enable_gold_fact: "true"`
trong `targets.prod.variables` của `bundles/databricks.yml`. CI triển khai target prod
sau khi PR cấu hình được merge.

### 2.2 Các bảng tổng hợp

```python
@dp.materialized_view(name=f"{GOLD}.kpi_daily")
def kpi_daily():
    return spark.sql(f"""
      SELECT order_date, source,
             count(DISTINCT order_id)               AS orders,
             count(*)                               AS items,
             count(DISTINCT user_id)                AS users,
             round(sum(revenue_sim), 2)             AS revenue_sim,
             round(avg(reordered), 4)               AS reorder_rate,
             round(count(*) / count(DISTINCT order_id), 2) AS avg_basket_size
      FROM {GOLD}.fact_order_lines GROUP BY order_date, source""")


@dp.materialized_view(name=f"{GOLD}.kpi_by_dow_hour")
def kpi_by_dow_hour():
    # Dùng cột gốc order_dow / order_hour_of_day, KHÔNG suy từ order_ts_sim (data_dictionary §4)
    return spark.sql(f"""
      SELECT order_dow, order_hour_of_day,
             count(DISTINCT order_id) AS orders, count(*) AS items, round(avg(reordered), 4) AS reorder_rate
      FROM {GOLD}.fact_order_lines GROUP BY ALL""")


@dp.materialized_view(name=f"{GOLD}.department_sales", cluster_by=["department_id", "order_date"])
def department_sales():
    return spark.sql(f"""
      SELECT order_date, department_id, aisle_id,
             count(DISTINCT order_id) AS orders, count(*) AS items,
             round(sum(revenue_sim), 2) AS revenue_sim,
             sum(reordered) AS reordered_items,
             sum(CASE WHEN discount_pct IS NOT NULL THEN 1 ELSE 0 END) AS promo_items
      FROM {GOLD}.fact_order_lines GROUP BY ALL""")


@dp.materialized_view(name=f"{GOLD}.product_360")
@dp.expect_or_fail("one_row_per_product", "n = 1")
def product_360():
    return spark.sql(f"""
      WITH s AS (SELECT product_id, count(*) AS times_ordered, round(avg(reordered), 4) AS reorder_rate,
                        round(sum(revenue_sim), 2) AS revenue_sim
                 FROM {GOLD}.fact_order_lines GROUP BY product_id)
      SELECT d.product_id, d.product_name, d.aisle_id, d.aisle, d.department_id, d.department,
             d.current_price, d.on_promotion, d.discount_pct,
             d.brand, d.nutriscore_grade, d.nova_group, d.energy_kcal_100g, d.sugars_100g,
             d.image_url, d.off_match_status, d.off_match_score,
             coalesce(s.times_ordered, 0) AS times_ordered, s.reorder_rate, s.revenue_sim,
             count(*) OVER (PARTITION BY d.product_id) AS n
      FROM {SILVER}.dim_product d LEFT JOIN s USING (product_id)""")


@dp.materialized_view(name=f"{GOLD}.product_pair_counts", cluster_by=["p1"])
def product_pair_counts():
    # Baseline DE cho trang 2. Luật kết hợp đầy đủ là gold.association_rules / recommendations của DS (ds/05 §4).
    return spark.sql(f"""
      WITH top AS (SELECT product_id FROM {GOLD}.product_360 ORDER BY times_ordered DESC LIMIT 5000),
           b AS (SELECT e.order_id, array_sort(collect_list(e.product_id)) AS items      -- giữ món thuộc top
                 FROM (SELECT order_id, explode(items) AS product_id
                       FROM {SILVER}.baskets WHERE split = 'prior') e
                 JOIN top USING (product_id)
                 GROUP BY e.order_id),
           n AS (SELECT count(*) AS n_baskets FROM {SILVER}.baskets WHERE split = 'prior'),
           pairs AS (SELECT p.p1, p.p2 FROM b
                     LATERAL VIEW explode(flatten(transform(items, (a, i) ->
                        transform(slice(items, i + 2, size(items)), c -> named_struct('p1', a, 'p2', c))))) t AS p),
           cnt AS (SELECT p1, p2, count(*) AS pair_count FROM pairs GROUP BY p1, p2 HAVING count(*) >= 50),
           single AS (SELECT product_id, times_ordered FROM {GOLD}.product_360)
      SELECT c.p1, c.p2, c.pair_count,
             c.pair_count / n.n_baskets                               AS support,
             c.pair_count / s1.times_ordered                          AS confidence_p1_to_p2,
             (c.pair_count * n.n_baskets) / (s1.times_ordered * s2.times_ordered) AS lift
      FROM cnt c CROSS JOIN n
      JOIN single s1 ON s1.product_id = c.p1 JOIN single s2 ON s2.product_id = c.p2""")
```

- `items` được `array_sort` lại sau khi lọc, nên luôn có `p1 < p2`, không cần `least` / `greatest`.
- `times_ordered` ở `product_360` đếm **mọi** đơn (prior + stream), còn `pair_count` chỉ đếm giỏ prior. Vì vậy `confidence` / `lift` ở đây là số **xấp xỉ** cho dashboard. Số chuẩn để đánh giá thuật toán do DS tính trên đúng split.
- Lọc top 5.000 sản phẩm và `HAVING ≥ 50` giữ phép đếm cặp trong khả năng của 4 vCPU. Ghi thời gian chạy vào benchmark.

### 2.3 Dim và bảng phân quyền

```python
@dp.materialized_view(name=f"{GOLD}.dim_date")
def dim_date():
    return spark.sql("""
      SELECT d AS date, year(d) AS year, month(d) AS month, weekofyear(d) AS week,
             date_format(d, 'yyyy-MM') AS year_month, dayofweek(d) AS iso_dow
      FROM (SELECT explode(sequence(DATE'2015-01-01', DATE'2026-04-30', INTERVAL 1 DAY)) AS d)""")


@dp.materialized_view(name=f"{GOLD}.dim_department")
def dim_department():
    return spark.sql(f"SELECT DISTINCT department_id, department FROM {SILVER}.dim_product")


@dp.materialized_view(name=f"{GOLD}.bi_user_access",
                      comment="RLS: email người xem → department được xem. Nguồn nhập tay, không nằm trong git")
@dp.expect_or_drop("valid_dept", "department_id IS NOT NULL")
def bi_user_access():
    return spark.read.table(f"{LANDING_META}.bi_access_seed")
```

**`bi_access_seed` không được commit lên git** (email là dữ liệu cá nhân). P3 nhập tay một lần trong SQL editor:

```sql
CREATE TABLE IF NOT EXISTS instacart.landing_meta.bi_access_seed (user_email STRING, department_id INT);
INSERT INTO instacart.landing_meta.bi_access_seed VALUES
  ('<email thành viên A>', 4),     -- produce
  ('<email thành viên B>', 16);    -- dairy eggs
-- Vai Executive không cần dòng ở đây: role đó không lọc (07 §5)
```

## 3. Incremental hay tính lại toàn bộ

| Cách | Cơ chế | Dùng ở dự án |
|---|---|---|
| MV tính lại toàn bộ | Mỗi update chạy lại câu truy vấn | **Mặc định trên classic compute.** `fact_order_lines` mất khoảng 10–15 phút, ghi vào benchmark |
| MV tính tăng dần (incremental refresh) | LDP dùng row tracking / CDF của nguồn để chỉ tính phần đổi | Chỉ có trên **serverless**. Nếu spike cho dùng serverless pipeline thì bật, rồi so thời gian |
| **Change Data Feed (CDF)** đọc tay | Đọc các dòng insert/update/delete giữa hai version của một bảng Delta | Cho tiêu thụ **ngoài** pipeline, ví dụ task `publish_serving` (dự phòng 2) chỉ đẩy thay đổi giá sang Azure SQL |

Ví dụ CDF: đẩy thay đổi giá ra ngoài mà không đọc lại cả bảng:

```sql
-- silver.product_prices_history đã bật delta.enableChangeDataFeed (03 §2.2)
SELECT product_id, unit_price, __START_AT, __END_AT, _change_type, _commit_version
FROM table_changes('instacart.silver.product_prices_history', 12)       -- từ version 12 trở đi
WHERE _change_type IN ('insert', 'update_postimage', 'delete');
```

Task lưu `last_version` đã xử lý vào `ops.cdf_progress`. Lần sau đọc từ `last_version + 1`, chỉ cập nhật mốc sau khi ghi đích thành công. Đây là cùng nguyên tắc "watermark tăng sau cùng" như CDC ở [01 §3.6](01_ingest_landing.md).

## 4. Kiểm tra gold khớp silver

```sql
-- Fact không nhân dòng do join giá / KM
SELECT count(*) = count(DISTINCT order_id, product_id) AS no_fanout,
       count(*) AS n FROM instacart.gold.fact_order_lines;                       -- true | 33.819.106

-- Mọi món đều có giá (SCD2 phủ từ 2015-01-01)
SELECT count(*) FROM instacart.gold.fact_order_lines WHERE unit_price IS NULL;  -- 0

-- Tổng ở bảng tổng hợp = tổng ở fact
SELECT (SELECT sum(items) FROM instacart.gold.department_sales)
     = (SELECT count(*)   FROM instacart.gold.fact_order_lines) AS items_match;  -- true
```

33.819.106 là tổng 32.434.489 món prior và 1.384.617 món train từ stream.

`src/tasks/reconcile_g4.sql` ghi bốn check `g4.fact.rows`, `g4.fact.no_fanout`,
`g4.fact.keys` và `g4.fact.unpriced` vào `ops.reconciliation`, rồi báo lỗi nếu có check đỏ.
Chạy file này sau `reconcile.sql` và `reconcile_g3.sql` đã đạt với cùng
`run_id`, `check_date`, profile và schema. Đây là phần đối soát fact của AD-DE-22.
Check tổng hợp `department_sales` ở trên dùng khi bảng tổng hợp đã có.

### 4.1 Đo thời gian refresh fact

Chọn update đã `COMPLETED`, giữ lại update ID và số dòng đối soát, rồi lấy mốc thời
gian của flow `fact_order_lines` trong `ops.event_log_batch`. SQL sau nhận tham số
STRING `catalog`, `ops_schema`, `update_id` từ môi trường đang đo.

```sql
WITH flow_events AS (
  SELECT timestamp AS event_ts,
    get_json_object(to_json(named_struct('v', origin)), '$.v.flow_name') AS flow_name,
    get_json_object(to_json(named_struct('v', origin)), '$.v.update_id') AS update_id,
    get_json_object(get_json_object(to_json(named_struct('v', details)), '$.v'), '$.flow_progress.status') AS flow_status
  FROM IDENTIFIER(
    '`' || replace(:catalog, '`', '``') || '`.`' || replace(:ops_schema, '`', '``') || '`.event_log_batch'
  )
  WHERE event_type = 'flow_progress'
), fact_times AS (
  SELECT min(CASE WHEN flow_status IN ('STARTING', 'RUNNING') THEN event_ts END) AS started_at,
         max(CASE WHEN flow_status = 'COMPLETED' THEN event_ts END) AS completed_at
  FROM flow_events
  WHERE update_id = :update_id
    AND (flow_name = 'fact_order_lines' OR endswith(replace(flow_name, '`', ''), '.fact_order_lines'))
)
SELECT started_at, completed_at,
       timestampdiff(MILLISECOND, started_at, completed_at) / 1000.0 AS refresh_seconds
FROM fact_times
WHERE started_at IS NOT NULL AND completed_at >= started_at;
```

Ghi cả thời gian flow và thời gian update trong báo cáo benchmark, kèm profile dữ liệu và
loại compute. Thời gian flow lấy từ event `STARTING` hoặc `RUNNING` đến `COMPLETED`,
còn thời gian update gồm khởi tạo compute và các flow khác.
[Pipeline event log](https://learn.microsoft.com/en-us/azure/databricks/ldp/monitor-event-logs).

## 5. Tối ưu lưu trữ và đọc/ghi (rubric 3.1, 3.4)

| Kỹ thuật | Áp dụng | Hiệu quả kỳ vọng |
|---|---|---|
| Delta thay CSV | Mọi tầng từ bronze | Nén cột, có thống kê min/max, bỏ qua file không liên quan khi lọc |
| **Liquid clustering** (`cluster_by`) | `fact_order_lines (order_date, department_id)`, `department_sales`, `order_items_prior (order_id)` | Lọc theo ngày / ngành chỉ đọc một phần file |
| OPTIMIZE + VACUUM | LDP tự chạy tác vụ bảo trì cho bảng của pipeline (trong vòng 24 giờ sau update) | Gộp file nhỏ; dọn file cũ quá 7 ngày |
| Không lưu nhị phân ảnh | `bronze.product_images` chỉ có metadata | Tiết kiệm dung lượng |
| Lifecycle landing → cool | Policy ở `10-core` | Giảm tiền lưu file thô ít đọc |

> Tác vụ bảo trì của LDP trên classic compute cũng khởi động cluster, tức là cũng chiếm quota. Đặt lịch batch sao cho cửa sổ bảo trì không trùng buổi demo.

### 5.1 Notebook benchmark → `ops.benchmark_results`

```python
# src/tasks/benchmark_formats.py  (chạy trên job cluster single-node theo policy → cấu hình cố định)
import time, uuid
from pyspark.sql import functions as F

SRC  = "instacart.silver.order_items_prior"
BASE = "/Volumes/instacart/ops/checkpoints/bench"     # hoặc một volume riêng trong instacart_sandbox
run  = str(uuid.uuid4())[:8]
df   = spark.table(SRC).select("order_id", "product_id", "add_to_cart_order", "reordered")
rows = []


def timed(fn):
    t = time.time(); fn(); return round(time.time() - t, 2)


def size_mb(path):
    return round(sum(f.size for f in dbutils.fs.ls(path) if not f.name.startswith("_")) / 2**20, 1)


for fmt in ["csv", "parquet", "delta", "delta_optimized"]:
    path = f"{BASE}/{run}/{fmt}"
    w = "delta" if fmt.startswith("delta") else fmt
    t_write = timed(lambda: df.write.format(w).mode("overwrite").option("header", "true").save(path))
    if fmt == "delta_optimized":
        t_write += timed(lambda: spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (product_id)"))
    reader = lambda: spark.read.format(w).option("header", "true").schema(df.schema).load(path)
    t_scan = timed(lambda: reader().agg(F.count("*")).collect())
    t_filter = timed(lambda: reader().where("product_id = 24852").agg(F.count("*")).collect())
    rows.append((run, fmt, df.count(), t_write, t_scan, t_filter, size_mb(path)))

(spark.createDataFrame(rows, "run_id string, format string, n_rows long, write_s double, "
                             "scan_s double, filter_s double, size_mb double")
      .withColumn("measured_at", F.current_timestamp())
      .write.mode("append").saveAsTable("instacart.ops.benchmark_results"))
dbutils.fs.rm(f"{BASE}/{run}", recurse=True)          # dọn dữ liệu thử
```

- Đường dẫn `delta.` cho phép `OPTIMIZE ... ZORDER BY` trên bảng không đăng ký trong catalog. Muốn thử liquid clustering thì ghi thành bảng trong `instacart_sandbox` với `CLUSTER BY (product_id)`.
- Chạy task **3 lần** (mỗi lần một `run_id`), báo cáo lấy trung vị theo định dạng. Ghi cấu hình máy (`Standard_D4ds_v4`, runtime) vào báo cáo.
- `size_mb` chỉ đếm file ở thư mục gốc. Với Delta, file dữ liệu nằm ngay ở gốc nên đếm đúng; CSV và Parquet của Spark cũng vậy.

## 6. Phục vụ Power BI

- **Mặc định:** Power BI kết nối serverless SQL warehouse `wh-instacart-bi`, chỉ đọc schema `gold` (và `ops` cho trang vận hành). Quyền đã cấp ở [devops/05 bước 5](../devops/05_databricks_platform.md).
- Thang dự phòng (gold → Azure SQL, hoặc `DeltaLake.Table`) ở [devops/06 §4](../devops/06_de_den_dashboard.md). Nếu dùng dự phòng 2, task `publish_serving` chép các bảng trong mục 1, **trừ** `fact_order_lines`, bằng JDBC `mode("overwrite")`. Bảng nhỏ nên ghi đè cả bảng là idempotent.
- Sau khi pipeline batch xong, task `refresh_bi` gọi API refresh semantic model ([07 §7](07_power_bi.md)).

## 7. Lỗi giữa chừng thì chạy lại thế nào

| Tình huống | Cách xử lý |
|---|---|
| Update gold lỗi | Chạy lại update (hoặc *Repair run* của job). MV tính lại, không có trạng thái dở |
| Sửa logic một bảng gold | Deploy rồi `refresh_selection` bảng đó. Các bảng phía sau trong đồ thị cũng tự refresh |
| Power BI đọc lúc gold đang refresh | Mỗi MV được thay bằng một commit Delta nguyên tử. Power BI thấy bản cũ hoặc bản mới, không thấy bản nửa vời |
| Benchmark lỗi giữa chừng | Chạy lại. Mỗi lần có `run_id` riêng; dòng của lần lỗi không được ghi vì `append` ở cuối |

## 8. Checklist (phần DE của G4)

- [ ] Các truy vấn ở mục 4 đều đạt.
- [ ] `SELECT * FROM instacart.ops.benchmark_results` có 4 định dạng × 3 lần.
- [ ] P4 (vai `bi`) đọc được `gold.*` qua warehouse. Không có tài khoản chỉ mang vai `bi` (P4 còn vai `ds`, được đọc silver), nên kiểm phần chặn bằng `SHOW GRANTS ON SCHEMA instacart.silver`: không có grant nào dành riêng cho vai `bi` (bảng quyền ở devops/05 bước 5).
- [ ] `gold.bi_user_access` có ít nhất 2 người, ở 2 department khác nhau.
