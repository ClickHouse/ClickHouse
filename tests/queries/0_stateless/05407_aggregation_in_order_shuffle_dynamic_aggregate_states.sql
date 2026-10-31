-- Shuffled aggregation-in-order must not be used over `Dynamic` or `JSON` columns while its buffer budget is
-- enabled: they can carry aggregate states whose payload lives on the heap (e.g. the hash set of `uniqExact`) in
-- variants that appear only in the data chunks, so the eligibility check on the header cannot see them (see 05401).

SET enable_parallel_replicas = 0;
SET read_in_order_use_virtual_row = 0;
-- The shuffle is disabled when `max_rows_to_group_by` is set (see 04514).
SET max_rows_to_group_by = 0;
SET merge_tree_min_rows_for_concurrent_read = 0;
SET merge_tree_min_bytes_for_concurrent_read = 0;

DROP TABLE IF EXISTS t_aio_shuffle_dynamic_state;
CREATE TABLE t_aio_shuffle_dynamic_state (k UInt64, d Dynamic, j JSON, v UInt64) ENGINE = MergeTree ORDER BY k;

SYSTEM STOP MERGES t_aio_shuffle_dynamic_state;
INSERT INTO t_aio_shuffle_dynamic_state SELECT number % 4, CAST(uniqExactState(number) AS Dynamic), '{"a" : 1}', sum(number) FROM numbers(10000) GROUP BY number % 4;
INSERT INTO t_aio_shuffle_dynamic_state SELECT number % 4, CAST(uniqExactState(number) AS Dynamic), '{"a" : 1}', sum(number) FROM numbers(10000, 10000) GROUP BY number % 4;

-- A `Dynamic` column in the input disables the shuffle while the budget is enabled.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, count(d) FROM t_aio_shuffle_dynamic_state GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- So does a `JSON` column.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, count(j) FROM t_aio_shuffle_dynamic_state GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- Without a budget there is nothing to account, so the shuffle is used.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, count(d) FROM t_aio_shuffle_dynamic_state GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1,
               aggregation_in_order_shuffle_max_buffered_bytes = 0);

-- Other columns of the same table are still shuffled.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, sum(v) FROM t_aio_shuffle_dynamic_state GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- Results are the same either way.
SELECT k, count(d), sum(v) FROM t_aio_shuffle_dynamic_state GROUP BY k ORDER BY k
SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1;
SELECT k, count(d), sum(v) FROM t_aio_shuffle_dynamic_state GROUP BY k ORDER BY k
SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1,
         aggregation_in_order_shuffle_max_buffered_bytes = 0;

DROP TABLE t_aio_shuffle_dynamic_state;
