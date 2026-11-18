# 08 · Chuẩn bị dữ liệu cho DS (silver → `ds_data`)

**Đầu vào:**
- Checklist G3 đã đạt ([03 §9](03_silver_cdc_scd_dq.md)): `silver.baskets` có đủ `prior` và `holdout`, stream đã phát xong.
- Schema `ds_data` và volume `ds_data.exports` đã có ([devops/05 bước 4](../devops/05_databricks_platform.md)).

**Đầu ra (schema `ds_data`, DE sở hữu, DS chỉ đọc):**

| Bảng / file | Nội dung | Ai dùng |
|---|---|---|
| `ds_data.data_versions` | Mỗi phiên bản dữ liệu một dòng: thời điểm tạo, số giỏ `prior` / `holdout` | Đối soát |
| `ds_data.baskets` | **Bản cố định** của `silver.baskets` theo `data_version` | Nguồn của mọi bảng dưới |
| `ds_data.item_index` | Mã hóa item thành `idx` (0 = phổ biến nhất) cho 3 cấp `product` / `aisle` / `department` | DS, view `gold.*` |
| `ds_data.transactions` | Giỏ hàng theo 3 cấp, `items array<int>` tăng dần, `scale_bucket` 0–99 | DApriori, DATID, FP-Growth |
| `ds_data.holdout_eval` | Đơn holdout chia thành `context` / `targets`, chia `val` / `test` | Đánh giá gợi ý (E7) |
| `/Volumes/instacart/ds_data/exports/<v>/<grain>/scale=<pct>/part-*.txt` + `<v>/_manifest.json` | File txt, mỗi dòng một giao dịch | Hadoop MapReduce / Spark ở máy local ([ds/03](../ds/03_hadoop_mapreduce_local.md)) |

**Xong khi:** checklist mục 7 đạt, và DS xác nhận đọc được (mốc G3 của DS, [ds/00 §5](../ds/00_tong_quan_ds.md)).

---

## 1. Vì sao DE làm phần này

- **Một nguồn dữ liệu cho mọi thực nghiệm.** Databricks và cụm Hadoop local đều đọc từ cùng `ds_data.transactions` (local đọc qua file txt xuất ra). Số cặp phổ biến của MapReduce vì vậy so được với số của DApriori.
- **Dữ liệu cố định.** `silver.baskets` là materialized view và có thể được tính lại khi pipeline chạy. Thực nghiệm kéo dài nhiều tuần thì cần một bản **không đổi**.
  - Không dùng `VERSION AS OF` trên MV, vì time travel trên bảng do pipeline quản lý không chắc được hỗ trợ.
  - Không dùng `CLONE`, vì clone không áp dụng cho MV / streaming table.
  - Dùng **`INSERT … SELECT` có chặn ghi lần hai**.
- **Quyền rõ ràng.** DS có `SELECT` trên `ds_data` và không sửa được dữ liệu đầu vào của chính mình.

## 2. Snapshot theo `data_version`

[`01_snapshot.sql`](../../src/tasks/ds_prepare/01_snapshot.sql) cố định ba đầu vào:

| Bảng | Dữ liệu theo phiên bản |
|---|---|
| `ds_data.baskets` | Giỏ prior/holdout từ `silver.baskets`, giữ tập `product_id` tăng dần |
| `ds_data.ds_prepare_products` | Khóa, tên, aisle và department từ `silver.dim_product` |
| `ds_data.ds_prepare_holdout` | Danh sách `product_id` theo `add_to_cart_order` của stream cho từng giỏ holdout |

`order_id`, `user_id`, `product_id` và khóa danh mục phải nằm trong miền INT dương. Script dừng khi
phát hiện giỏ trùng, sản phẩm không có trong danh mục, hoặc thứ tự món stream thiếu, trùng hay lệch tập
sản phẩm của giỏ. Khi cả ba phần đã ghi và kiểm đủ prior/holdout, script ghi `data_versions` với số giỏ,
nguồn và thời điểm tạo. Metadata này xác nhận snapshot sẵn sàng cho bước mã hóa.

Mỗi phần chỉ insert khi chưa có dữ liệu của phiên bản đó. Nếu lỗi giữa các phần, repair dùng cùng
catalog, schema và nguồn được giữ ổn định từ lần đầu. Delta commit theo từng bảng, nên cần giữ nguồn
silver đứng yên đến khi snapshot hoàn tất và chỉ chạy một writer vào schema DS. Các phần đã ghi được
sử dụng lại. Nếu cần thay dữ liệu đầu vào đã cố định, dùng một phiên bản mới và lưu lại nguyên nhân.

## 3. Mã hóa item và giao dịch theo 3 cấp

[`02_index_transactions.sql`](../../src/tasks/ds_prepare/02_index_transactions.sql) dùng dữ liệu snapshot:

- `item_index` có ba cấp `product`, `aisle`, `department`. `freq_prior` đếm **số giỏ prior chứa item**.
  `idx` đánh liên tục từ 0 theo tần suất giảm dần và khóa số tăng dần khi hòa. DS lọc top-N bằng `idx < N`.
- `transactions.items` là tập `idx` tăng dần. Một đơn mua nhiều sản phẩm cùng aisle hoặc department
  vẫn có một `idx` của nhóm đó. Holdout chỉ giữ item đã có trong prior theo từng cấp. Nếu một giỏ không
  còn item ở một cấp, giỏ đó không tạo giao dịch ở cấp đó.
- `scale_bucket = pmod(xxhash64(order_id), 100)`, với `order_id` kiểu INT. Các ngưỡng 10, 25, 50, 100
  tạo tập con lồng nhau và tất định ([ds/01 §1](../ds/01_du_lieu_thuc_nghiem.md)).
- `REPLACE WHERE data_version = :v` tính lại riêng index và transactions của phiên bản đó. Danh mục
  đã cố định, nên thay đổi tên hoặc phân loại ở silver không đổi mã hay giao dịch của `v1`.

## 4. Tập đánh giá `ds_data.holdout_eval`

[`03_holdout.sql`](../../src/tasks/ds_prepare/03_holdout.sql) mã hóa danh sách holdout đã cố định ở bước 01.
Lọc sản phẩm chưa có trong prior rồi chọn đơn còn ít nhất hai item. `context` là nửa đầu làm tròn lên,
`targets` là phần còn lại **theo thứ tự bỏ vào giỏ**. Sau khi chia, cả hai mảng được sắp tăng dần theo
`idx` để DS so tập bằng phép `=`. Hash `xxhash64(order_id, 'eval')` chia đơn vào `val` hoặc `test`.
Bảng chỉ insert lần đầu cho mỗi phiên bản. Nếu kết quả bằng 0 dòng, replay cũng cho cùng kết quả vì
đầu vào đã cố định.

## 5. Xuất file cho Hadoop local

```python
# src/tasks/ds_prepare/export_txt.py  (job cluster single-node của DE)
import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

VOL = "/Volumes/instacart/ds_data/exports"
GRAINS, SCALES = ("product", "aisle", "department"), (10, 25, 50, 100)

p = argparse.ArgumentParser()
p.add_argument("--data-version", default="v1")
v = p.parse_args().data_version
spark = SparkSession.builder.getOrCreate()

tx = spark.table("instacart.ds_data.transactions").where((F.col("data_version") == v) & (F.col("split") == "prior"))
manifest = {"data_version": v, "format": "txt: idx cách nhau bằng dấu cách, mỗi dòng một giao dịch"}
for grain in GRAINS:
    manifest[grain] = {}
    for scale in SCALES:
        part = tx.where((F.col("grain") == grain) & (F.col("scale_bucket") < scale))
        path = f"{VOL}/{v}/{grain}/scale={scale}"
        (part.select(F.concat_ws(" ", F.transform("items", lambda x: x.cast("string"))).alias("value"))
             .repartition(max(1, scale // 25))                           # vài file; HDFS tự chia block 8 MB lúc put
             .write.mode("overwrite").text(path))
        n_written = spark.read.text(path).count()
        n_expected = part.count()
        assert n_written == n_expected, (grain, scale, n_written, n_expected)
        manifest[grain][str(scale)] = n_expected

# Manifest ghi SAU CÙNG: có manifest nghĩa là mọi file đã đủ (vai trò như _SUCCESS)
with open(f"{VOL}/{v}/_manifest.json", "w") as f:
    json.dump(manifest, f, indent=2)
```

- Volume `ds_data.exports` trỏ tới `abfss://ds-data@<lake>.dfs.core.windows.net/exports`. Đường dẫn `/Volumes/…/exports/v1/product/scale=10/` tương ứng với blob `ds-data/exports/v1/product/scale=10/`, nơi AzCopy ở máy local tải về ([ds/03 §3](../ds/03_hadoop_mapreduce_local.md)).
- `mode("overwrite")` trên đường dẫn tất định: chạy lại thì thay đúng thư mục đó bằng nội dung y hệt, vì `transactions` của một phiên bản không đổi.
- Volume hỗ trợ ghi file bằng `open()` của Python (FUSE), nên manifest không cần `dbutils`.

## 6. Job `ds-prepare`

```yaml
# bundles/resources/jobs_ds.yml  (phần của DE)
resources:
  jobs:
    ds_prepare:
      name: instacart-ds-prepare
      max_concurrent_runs: 1
      parameters:
        - { name: data_version, default: v1 }
      tasks:
        - task_key: prepare_tables                      # chạy 01 → 02 → 03 theo thứ tự
          job_cluster_key: sn
          spark_python_task:
            python_file: ../../src/tasks/ds_prepare/run_sql.py
            parameters: ["--data-version", "{{job.parameters.data_version}}",
                         "01_snapshot.sql", "02_index_transactions.sql", "03_holdout.sql"]
        - task_key: export_txt
          depends_on: [{ task_key: prepare_tables }]
          job_cluster_key: sn
          spark_python_task:
            python_file: ../../src/tasks/ds_prepare/export_txt.py
            parameters: ["--data-version", "{{job.parameters.data_version}}"]
        - task_key: check
          depends_on: [{ task_key: export_txt }]
          job_cluster_key: sn
          spark_python_task:
            python_file: ../../src/tasks/ds_prepare/run_sql.py
            parameters: ["--data-version", "{{job.parameters.data_version}}", "04_check.sql"]
      job_clusters:
        - job_cluster_key: sn
          new_cluster:                          # khai đủ trường fixed của policy, không dùng apply_policy_default_values (devops/05 §3 bước 6)
            policy_id: ${var.policy_id}
            spark_version: 15.4.x-scala2.12
            node_type_id: ${var.node_type_id}
            num_workers: 0
            data_security_mode: SINGLE_USER
            spark_conf:
              spark.databricks.cluster.profile: singleNode
              spark.master: "local[*]"
            custom_tags: { ResourceClass: SingleNode, cost_owner: p2 }
```

- [`run_sql.py`](../../src/tasks/ds_prepare/run_sql.py) chạy 01 → 02 → 03 theo mặc định trong cùng SparkSession.
  Runner tách `;` bên ngoài chuỗi, identifier và comment rồi dừng tại câu lỗi. `:v` được bind qua
  `spark.sql(..., args=...)`. Delta OSS 3.3 tách predicate `REPLACE WHERE` trước khi bind, nên runner chỉ
  render phiên bản đã kiểm định dạng ở hai predicate ghi đè. Tên bảng đi qua `IDENTIFIER` đã quote từng phần.
  Dùng `--catalog`, `--silver-schema` và `--ds-schema` để cô lập dev/local. Có thể truyền danh sách tên file
  trong thư mục `ds_prepare` để repair một bước, hoặc chạy `04_check.sql` sau khi AD-DE-21 bổ sung file đó.
- AD-DE-20 kiểm ba file trên Spark 3.5 và Delta 3.3 local với fixture. AD-DE-21 phụ trách bundle, file txt,
  check G3 và nghiệm thu `v1` trên workspace với dữ liệu đầy đủ.
- **Không đặt lịch.** P2 chạy tay một lần sau G3 (`databricks bundle run ds_prepare`), và chạy lại khi tạo phiên bản mới.
- Dùng job cluster single-node (4 vCPU), nên **không chạy cùng lúc** với job batch hay pipeline stream.

## 7. Checklist (mốc G3 của DS)

```sql
-- src/tasks/ds_prepare/04_check.sql   (assert_true: sai thì task đỏ)
SELECT assert_true(n_prior = (SELECT count(*) FROM instacart.silver.baskets WHERE split = 'prior'),
                   'n_prior lệch silver.baskets')
FROM instacart.ds_data.data_versions WHERE data_version = :v;

SELECT assert_true(count(DISTINCT grain) = 3 AND min(n) = max(n), 'mỗi cấp phải có cùng số giỏ prior')
FROM (SELECT grain, count(*) AS n FROM instacart.ds_data.transactions
      WHERE data_version = :v AND split = 'prior' GROUP BY grain);

SELECT assert_true(sum(n_items) = (SELECT sum(n_items) FROM instacart.ds_data.baskets
                                   WHERE data_version = :v AND split = 'prior'),
                   'tổng số món cấp product phải bằng snapshot (mỗi món 1 lần / đơn)')
FROM instacart.ds_data.transactions WHERE data_version = :v AND grain = 'product' AND split = 'prior';

SELECT assert_true(count_if(grain = 'department') = 21 AND count_if(grain = 'aisle') = 134,
                   'số item cấp department / aisle')
FROM instacart.ds_data.item_index WHERE data_version = :v;

SELECT assert_true(count(*) > 0 AND abs(count_if(eval_part = 'val') - count_if(eval_part = 'test')) < 0.05 * count(*),
                   'holdout_eval rỗng hoặc chia val/test lệch')
FROM instacart.ds_data.holdout_eval WHERE data_version = :v;
```

**Ghi vào báo cáo** (chạy tay, không chặn):

| Số liệu | Truy vấn | Kỳ vọng |
|---|---|---|
| Số giỏ `prior` / `holdout` | `ds_data.data_versions` | 3.214.874 / 131.209 (khớp checklist G3) |
| Số item cấp product | `count(*) FROM item_index WHERE grain = 'product'` | Khoảng 49.677 (sản phẩm có trong prior) |
| Số đơn holdout dùng được để đánh giá | `count(*) FROM holdout_eval` | Ít hơn 131.209 một chút (bỏ đơn 1 món) |
| Số món holdout bị bỏ vì không có trong prior | Đếm `stream_order_items` không join được `item_index` | Rất nhỏ; ghi con số thật |
| Số dòng file xuất | `_manifest.json` | Bằng `transactions` theo cấp × quy mô (task tự `assert`) |

- [ ] Task `check` xanh.
- [ ] `_manifest.json` có đủ 3 cấp × 4 quy mô.
- [ ] P4 (vai `ds`, không thuộc `de`) đọc được `ds_data.*` và volume `exports`; **không** ghi được (`INSERT` báo `PERMISSION_DENIED`). Không thử bằng P1 vì P1 thuộc `de`.
- [ ] Báo DS (P4) rằng `v1` đã sẵn sàng; ghi ngày tạo `v1` vào [ds/00 §5](../ds/00_tong_quan_ds.md).

## 8. Lỗi giữa chừng thì chạy lại thế nào

| Tình huống | Cách chạy lại | Vì sao không trùng / không mất |
|---|---|---|
| `prepare_tables` lỗi trong hoặc giữa 01 → 03 | Repair run cùng phiên bản, catalog và schema ban đầu | Bước 01 giữ phần snapshot đã commit, bước 02 chỉ thay phần của phiên bản đang chạy, bước 03 giữ tập đánh giá đã tạo |
| `export_txt` lỗi ở một quy mô | Repair run | `mode("overwrite")` trên đường dẫn tất định; manifest chỉ ghi khi **mọi** file đủ |
| `check` báo lệch | Điều tra theo thông báo; nếu silver sai thì sửa silver rồi tạo **`v2`** | `v1` không bị sửa ngầm; DS biết rõ đang dùng phiên bản nào |
| Lỡ chạy job với `data_version` đã có | Không cần làm gì | Snapshot và holdout bị chặn ghi; index/transactions tính lại ra y hệt |
| File trên volume bị xóa nhầm | Chạy lại riêng task `export_txt` | Tạo lại từ `ds_data.transactions` |
