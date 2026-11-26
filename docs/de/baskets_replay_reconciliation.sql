-- Chạy trong schema Silver dev nghiệm thu sau khi update batch hoàn tất.
WITH before_replay AS (
  SELECT order_id, user_id, split, items, n_items
  FROM basket_acceptance_before
),
after_replay AS (
  SELECT order_id, user_id, split, items, n_items
  FROM baskets
),
missing_or_changed AS (
  SELECT * FROM before_replay
  EXCEPT ALL
  SELECT * FROM after_replay
),
added_or_changed AS (
  SELECT * FROM after_replay
  EXCEPT ALL
  SELECT * FROM before_replay
),
basket_counts AS (
  SELECT split, count(*) AS baskets, count(DISTINCT order_id) AS distinct_orders,
         sum(n_items) AS total_items
  FROM after_replay
  GROUP BY split
),
replay_diff AS (
  SELECT
    (SELECT count(*) FROM missing_or_changed) AS missing_or_changed,
    (SELECT count(*) FROM added_or_changed) AS added_or_changed
)
SELECT split, baskets, distinct_orders, total_items, missing_or_changed, added_or_changed
FROM basket_counts
CROSS JOIN replay_diff
ORDER BY split;
