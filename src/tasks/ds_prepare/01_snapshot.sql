-- Thực thi bởi run_sql.py. Một writer cho mỗi schema DS; giữ nguồn ổn định trong lần tạo đầu.
CREATE TABLE IF NOT EXISTS IDENTIFIER(:data_versions) (
  data_version STRING NOT NULL,
  created_at TIMESTAMP NOT NULL,
  n_prior BIGINT NOT NULL,
  n_holdout BIGINT NOT NULL,
  source STRING NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS IDENTIFIER(:baskets) (
  data_version STRING NOT NULL,
  order_id INT NOT NULL,
  user_id INT NOT NULL,
  split STRING NOT NULL,
  items ARRAY<INT> NOT NULL,
  n_items INT NOT NULL
) USING DELTA CLUSTER BY (data_version, split);

CREATE TABLE IF NOT EXISTS IDENTIFIER(:frozen_products) (
  data_version STRING NOT NULL,
  product_id INT NOT NULL,
  product_name STRING NOT NULL,
  aisle_id INT NOT NULL,
  aisle STRING NOT NULL,
  department_id INT NOT NULL,
  department STRING NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS IDENTIFIER(:frozen_holdout) (
  data_version STRING NOT NULL,
  order_id INT NOT NULL,
  ordered_products ARRAY<INT> NOT NULL
) USING DELTA;

SELECT assert_true(
  count(*) <= 1 AND count_if(source <> :source_baskets) = 0,
  'Phiên bản đã dùng nguồn khác hoặc metadata bị trùng'
)
FROM IDENTIFIER(:data_versions) WHERE data_version = :v;

INSERT INTO IDENTIFIER(:baskets)
WITH normalized AS (
  SELECT try_cast(order_id AS INT) AS order_id,
         try_cast(user_id AS INT) AS user_id, split, n_items,
         array_sort(transform(items, p -> try_cast(p AS INT))) AS items,
         count(*) OVER (PARTITION BY order_id) AS order_rows
  FROM IDENTIFIER(:source_baskets)
  WHERE NOT EXISTS (SELECT 1 FROM IDENTIFIER(:baskets) WHERE data_version = :v)
    AND NOT EXISTS (SELECT 1 FROM IDENTIFIER(:data_versions) WHERE data_version = :v)
)
SELECT :v,
       CASE WHEN order_rows = 1 AND order_id > 0 AND user_id > 0
                  AND split IN ('prior', 'holdout') AND n_items > 0 AND n_items = size(items)
                  AND size(array_distinct(items)) = n_items AND forall(items, p -> p IS NOT NULL AND p > 0)
            THEN order_id
            ELSE CAST(raise_error('Giỏ nguồn trùng hoặc chứa khóa, split hay món không hợp lệ') AS INT) END,
       user_id, split, items, CAST(n_items AS INT)
FROM normalized;

SELECT assert_true(count_if(split = 'prior') > 0 AND count_if(split = 'holdout') > 0,
                   'Snapshot cần giỏ prior và holdout sau G3')
FROM IDENTIFIER(:baskets) WHERE data_version = :v;

-- Danh mục và vị trí giỏ được giữ cùng phiên bản để 02/03 không đọc silver đã thay đổi.
INSERT INTO IDENTIFIER(:frozen_products)
WITH products AS (
  SELECT try_cast(product_id AS INT) AS product_id, product_name,
         try_cast(aisle_id AS INT) AS aisle_id, aisle,
         try_cast(department_id AS INT) AS department_id, department,
         count(*) OVER (PARTITION BY product_id) AS product_rows
  FROM IDENTIFIER(:source_products)
  WHERE NOT EXISTS (SELECT 1 FROM IDENTIFIER(:frozen_products) WHERE data_version = :v)
    AND NOT EXISTS (SELECT 1 FROM IDENTIFIER(:data_versions) WHERE data_version = :v)
)
SELECT :v,
       CASE WHEN product_rows = 1 AND product_id > 0 AND aisle_id > 0 AND department_id > 0
                  AND product_name IS NOT NULL AND aisle IS NOT NULL AND department IS NOT NULL
            THEN product_id
            ELSE CAST(raise_error('dim_product trùng hoặc thiếu khóa, tên hay nhóm hợp lệ') AS INT) END,
       product_name, aisle_id, aisle, department_id, department
FROM products;

SELECT assert_true(count(*) = 0, 'Snapshot có sản phẩm thiếu trong danh mục cố định')
FROM (
  SELECT DISTINCT explode(items) AS product_id
  FROM IDENTIFIER(:baskets) WHERE data_version = :v
) b
LEFT ANTI JOIN (
  SELECT product_id FROM IDENTIFIER(:frozen_products) WHERE data_version = :v
) d USING (product_id);

INSERT INTO IDENTIFIER(:frozen_holdout)
WITH holdout AS (
  SELECT order_id, items, n_items
  FROM IDENTIFIER(:baskets) WHERE data_version = :v AND split = 'holdout'
    AND NOT EXISTS (SELECT 1 FROM IDENTIFIER(:frozen_holdout) WHERE data_version = :v)
    AND NOT EXISTS (SELECT 1 FROM IDENTIFIER(:data_versions) WHERE data_version = :v)
), paired AS (
  SELECT b.order_id, b.items, b.n_items,
         count(DISTINCT o.add_to_cart_order) AS distinct_positions,
         array_sort(collect_list(named_struct('pos', try_cast(o.add_to_cart_order AS INT),
                                              'product_id', try_cast(o.product_id AS INT)))) AS seq
  FROM holdout b
  LEFT JOIN IDENTIFIER(:source_order_items) o ON b.order_id = o.order_id AND o.source = 'stream'
  GROUP BY b.order_id, b.items, b.n_items
)
SELECT :v, order_id,
       CASE WHEN distinct_positions = n_items AND size(seq) = n_items
                  AND forall(seq, x -> x.pos IS NOT NULL AND x.pos > 0)
                  AND array_sort(transform(seq, x -> x.product_id)) = items
            THEN transform(seq, x -> x.product_id)
            ELSE CAST(raise_error('Vị trí món stream trùng, sai hoặc không khớp giỏ holdout') AS ARRAY<INT>) END
FROM paired;

SELECT assert_true(
  count(*) = (SELECT count(*) FROM IDENTIFIER(:baskets) WHERE data_version = :v AND split = 'holdout'),
  'Thiếu thứ tự giỏ holdout trong snapshot'
)
FROM IDENTIFIER(:frozen_holdout) WHERE data_version = :v;

MERGE INTO IDENTIFIER(:data_versions) t
USING (
  SELECT :v AS data_version, current_timestamp() AS created_at,
         count_if(split = 'prior') AS n_prior, count_if(split = 'holdout') AS n_holdout,
         :source_baskets AS source
  FROM IDENTIFIER(:baskets) WHERE data_version = :v
) s
ON t.data_version = s.data_version
WHEN NOT MATCHED THEN INSERT *;
