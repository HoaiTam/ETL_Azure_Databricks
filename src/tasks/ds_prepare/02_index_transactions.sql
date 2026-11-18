SELECT assert_true(count(*) = 1, 'Chạy 01_snapshot.sql trước khi mã hóa giỏ')
FROM IDENTIFIER(:data_versions) WHERE data_version = :v;

CREATE TABLE IF NOT EXISTS IDENTIFIER(:item_index) (
  data_version STRING NOT NULL,
  grain STRING NOT NULL,
  item_key STRING NOT NULL,
  idx INT NOT NULL,
  item_name STRING NOT NULL,
  freq_prior BIGINT NOT NULL
) USING DELTA CLUSTER BY (data_version, grain);

CREATE TABLE IF NOT EXISTS IDENTIFIER(:transactions) (
  data_version STRING NOT NULL,
  grain STRING NOT NULL,
  order_id INT NOT NULL,
  split STRING NOT NULL,
  items ARRAY<INT> NOT NULL,
  n_items INT NOT NULL,
  scale_bucket INT NOT NULL
) USING DELTA CLUSTER BY (data_version, grain, scale_bucket);

INSERT INTO IDENTIFIER(:item_index) REPLACE WHERE data_version = :v
WITH lines AS (
  SELECT order_id, explode(items) AS product_id
  FROM IDENTIFIER(:baskets) WHERE data_version = :v AND split = 'prior'
), keyed AS (
  SELECT b.order_id, k.grain, k.item_id, k.item_name
  FROM lines b JOIN IDENTIFIER(:frozen_products) d ON b.product_id = d.product_id AND d.data_version = :v
  LATERAL VIEW stack(3, 'product', d.product_id, d.product_name,
                        'aisle', d.aisle_id, d.aisle,
                        'department', d.department_id, d.department) k AS grain, item_id, item_name
), frequencies AS (
  SELECT grain, item_id, min(item_name) AS item_name, count(DISTINCT order_id) AS freq_prior
  FROM keyed GROUP BY grain, item_id
)
SELECT :v, grain, CAST(item_id AS STRING),
       CAST(row_number() OVER (PARTITION BY grain ORDER BY freq_prior DESC, item_id) - 1 AS INT),
       item_name, freq_prior
FROM frequencies;

INSERT INTO IDENTIFIER(:transactions) REPLACE WHERE data_version = :v
WITH lines AS (
  SELECT order_id, split, explode(items) AS product_id
  FROM IDENTIFIER(:baskets) WHERE data_version = :v
), keyed AS (
  SELECT b.order_id, b.split, k.grain, CAST(k.item_id AS STRING) AS item_key
  FROM lines b JOIN IDENTIFIER(:frozen_products) d ON b.product_id = d.product_id AND d.data_version = :v
  LATERAL VIEW stack(3, 'product', d.product_id, 'aisle', d.aisle_id, 'department', d.department_id)
    k AS grain, item_id
), grouped AS (
  SELECT k.grain, k.order_id, k.split, array_sort(collect_set(i.idx)) AS items
  FROM keyed k JOIN IDENTIFIER(:item_index) i
    ON i.data_version = :v AND i.grain = k.grain AND i.item_key = k.item_key
  GROUP BY k.grain, k.order_id, k.split
)
SELECT :v, grain, order_id, split, items, size(items), CAST(pmod(xxhash64(order_id), 100) AS INT)
FROM grouped;
