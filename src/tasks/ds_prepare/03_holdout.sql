SELECT assert_true(count(DISTINCT grain) = 3, 'Chạy 02_index_transactions.sql trước holdout')
FROM IDENTIFIER(:item_index) WHERE data_version = :v;

CREATE TABLE IF NOT EXISTS IDENTIFIER(:holdout_eval) (
  data_version STRING NOT NULL,
  order_id INT NOT NULL,
  eval_part STRING NOT NULL,
  context ARRAY<INT> NOT NULL,
  targets ARRAY<INT> NOT NULL
) USING DELTA;

INSERT INTO IDENTIFIER(:holdout_eval)
WITH lines AS (
  SELECT h.order_id, line.pos, line.product_id
  FROM IDENTIFIER(:frozen_holdout) h LATERAL VIEW posexplode(h.ordered_products) line AS pos, product_id
  WHERE h.data_version = :v
    AND NOT EXISTS (SELECT 1 FROM IDENTIFIER(:holdout_eval) WHERE data_version = :v)
), ordered AS (
  SELECT l.order_id, transform(array_sort(collect_list(named_struct('pos', l.pos, 'idx', i.idx))), x -> x.idx) AS items
  FROM lines l JOIN IDENTIFIER(:item_index) i
    ON i.data_version = :v AND i.grain = 'product' AND i.item_key = CAST(l.product_id AS STRING)
  GROUP BY l.order_id HAVING count(*) >= 2
), split_point AS (
  SELECT order_id, items, CAST(ceil(size(items) / 2.0) AS INT) AS context_size FROM ordered
)
SELECT :v, order_id, CASE WHEN pmod(xxhash64(order_id, 'eval'), 2) = 0 THEN 'val' ELSE 'test' END,
       array_sort(slice(items, 1, context_size)),
       array_sort(slice(items, context_size + 1, size(items) - context_size))
FROM split_point;
