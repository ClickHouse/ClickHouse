-- Shuffled aggregation-in-order must not be used over aggregate states that keep their payload on the heap
-- while its buffer budget is enabled: the budget measures the columns and the arenas of a buffered chunk, so
-- e.g. the hash set of a `uniqExact` state (grown through `HashTableAllocator`, outside any arena) would be
-- invisible to it. States whose payload lives in arenas (`groupArray`, see 04671) still use the shuffle.

SET enable_parallel_replicas = 0;
SET read_in_order_use_virtual_row = 0;
-- The shuffle is disabled when `max_rows_to_group_by` is set (see 04514).
SET max_rows_to_group_by = 0;
SET merge_tree_min_rows_for_concurrent_read = 0;
SET merge_tree_min_bytes_for_concurrent_read = 0;

DROP TABLE IF EXISTS t_aio_shuffle_heap_state;
CREATE TABLE t_aio_shuffle_heap_state (k UInt64, u AggregateFunction(uniqExact, UInt64), a AggregateFunction(groupArray, UInt64),
    f AggregateFunction(uniqExactForEach, Array(UInt64)))
ENGINE = AggregatingMergeTree ORDER BY k;

SYSTEM STOP MERGES t_aio_shuffle_heap_state;
INSERT INTO t_aio_shuffle_heap_state SELECT number % 4, uniqExactState(number), groupArrayState(number), uniqExactForEachState([number]) FROM numbers(10000) GROUP BY number % 4;
INSERT INTO t_aio_shuffle_heap_state SELECT number % 4, uniqExactState(number), groupArrayState(number), uniqExactForEachState([number]) FROM numbers(10000, 10000) GROUP BY number % 4;
INSERT INTO t_aio_shuffle_heap_state SELECT number % 4, uniqExactState(number), groupArrayState(number), uniqExactForEachState([number]) FROM numbers(20000, 10000) GROUP BY number % 4;

-- A heap-allocating state in the input disables the shuffle while the budget is enabled.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, uniqExactMerge(u) FROM t_aio_shuffle_heap_state GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- So does one nested in a combinator whose own states live in the arena.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, uniqExactForEachMerge(f) FROM t_aio_shuffle_heap_state GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- Without a budget there is nothing to account, so the shuffle is used.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, uniqExactMerge(u) FROM t_aio_shuffle_heap_state GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1,
               aggregation_in_order_shuffle_max_buffered_bytes = 0);

-- Arena-allocated states are still shuffled.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, length(groupArrayMerge(a)) FROM t_aio_shuffle_heap_state GROUP BY k
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- Results are the same either way.
SELECT k, uniqExactMerge(u), length(groupArrayMerge(a)), uniqExactForEachMerge(f) FROM t_aio_shuffle_heap_state GROUP BY k ORDER BY k
SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1;
SELECT k, uniqExactMerge(u), length(groupArrayMerge(a)), uniqExactForEachMerge(f) FROM t_aio_shuffle_heap_state GROUP BY k ORDER BY k
SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1,
         aggregation_in_order_shuffle_max_buffered_bytes = 0;

DROP TABLE t_aio_shuffle_heap_state;
