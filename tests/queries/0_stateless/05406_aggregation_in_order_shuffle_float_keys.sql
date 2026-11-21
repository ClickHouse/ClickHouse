-- Shuffled aggregation-in-order routes a row by the hash of its `GROUP BY` keys, which hashes the stored bits,
-- while the in-order aggregation finds group boundaries by comparison. For floating-point keys these disagree:
-- `+0.0` and `-0.0` compare equal and form one in-order group, but hash differently, so the shuffle would split
-- that group between shards. The shuffle must not be used for such keys.

SET enable_parallel_replicas = 0;
SET read_in_order_use_virtual_row = 0;
-- The shuffle is disabled when `max_rows_to_group_by` is set (see 04514).
SET max_rows_to_group_by = 0;
SET merge_tree_min_rows_for_concurrent_read = 0;
SET merge_tree_min_bytes_for_concurrent_read = 0;

DROP TABLE IF EXISTS t_aio_shuffle_float_keys;
DROP TABLE IF EXISTS t_aio_shuffle_int_keys;
CREATE TABLE t_aio_shuffle_float_keys (f Float64, n Nullable(Float32)) ENGINE = MergeTree ORDER BY (f, n)
SETTINGS allow_nullable_key = 1;
CREATE TABLE t_aio_shuffle_int_keys (i UInt64) ENGINE = MergeTree ORDER BY i;

-- Several parts, so the in-order read has several streams. In every part the run of zeros starts with `+0.0`.
SYSTEM STOP MERGES t_aio_shuffle_float_keys;
SYSTEM STOP MERGES t_aio_shuffle_int_keys;
INSERT INTO t_aio_shuffle_float_keys VALUES (0.0, 0.0), (-0.0, -0.0), (1.5, NULL);
INSERT INTO t_aio_shuffle_float_keys VALUES (0.0, 0.0), (-0.0, -0.0), (2.5, 2.5);
INSERT INTO t_aio_shuffle_int_keys VALUES (1), (2);
INSERT INTO t_aio_shuffle_int_keys VALUES (2), (3);

-- An integer key is shuffled.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT i, count() FROM t_aio_shuffle_int_keys GROUP BY i
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- A floating-point key is not, neither directly nor inside `Nullable`.
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT f, count() FROM t_aio_shuffle_float_keys GROUP BY f
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);
SELECT countIf(explain LIKE '%BufferedShardByHashTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT f, n, count() FROM t_aio_shuffle_float_keys GROUP BY f, n
      SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1);

-- The signed zeros form one group, as with the shuffle disabled.
SELECT f, count() FROM t_aio_shuffle_float_keys GROUP BY f ORDER BY f
SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 1;
SELECT f, count() FROM t_aio_shuffle_float_keys GROUP BY f ORDER BY f
SETTINGS max_threads = 8, optimize_aggregation_in_order = 1, aggregation_in_order_shuffle = 0;

DROP TABLE t_aio_shuffle_float_keys;
DROP TABLE t_aio_shuffle_int_keys;
