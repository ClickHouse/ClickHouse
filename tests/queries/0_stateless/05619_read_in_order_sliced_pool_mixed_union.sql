-- Tags: no-parallel-replicas
-- The sliced pool is used for local reading only.

-- The merge over a union neither buffers nor reads ahead only when every input of the union reads
-- through the sliced pool, which does both on the reading side. An input read any other way keeps the
-- merge's buffering and read-ahead.

SET optimize_read_in_order = 1;
SET read_in_order_use_virtual_row = 1;
SET read_in_order_use_sliced_pool = 1;
SET read_in_order_use_buffering = 1;
SET max_threads = 4;
SET max_insert_threads = 1;
SET materialize_statistics_on_insert = 0;
-- A table with more parts than this is merged in two levels and is not read through the pool.
SET read_in_order_two_level_merge_threshold = 2;

DROP TABLE IF EXISTS t_sliced_union_one_part;
DROP TABLE IF EXISTS t_sliced_union_many_parts;

CREATE TABLE t_sliced_union_one_part (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_sliced_union_one_part SELECT number FROM numbers(1000);

CREATE TABLE t_sliced_union_many_parts (k UInt64) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_sliced_union_many_parts;
INSERT INTO t_sliced_union_many_parts SELECT number FROM numbers(1000);
INSERT INTO t_sliced_union_many_parts SELECT number FROM numbers(1000, 1000);
INSERT INTO t_sliced_union_many_parts SELECT number FROM numbers(2000, 1000);

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_sliced_union_many_parts' AND active;

-- Every input reads through the pool: the merge consumes the lanes strictly on demand.
SELECT 'all inputs sliced: routers', countIf(explain LIKE '%MergeTreeInOrderSliceRouter%'), 'read-ahead', countIf(explain LIKE '%VirtualRowReadAhead%')
FROM (EXPLAIN PIPELINE SELECT k FROM (SELECT k FROM t_sliced_union_one_part UNION ALL SELECT k FROM t_sliced_union_one_part) ORDER BY k);

-- One input is merged in two levels: the merge keeps reading ahead for it.
SELECT 'one input sliced: routers', countIf(explain LIKE '%MergeTreeInOrderSliceRouter%'), 'read-ahead', countIf(explain LIKE '%VirtualRowReadAhead%')
FROM (EXPLAIN PIPELINE SELECT k FROM (SELECT k FROM t_sliced_union_one_part UNION ALL SELECT k FROM t_sliced_union_many_parts) ORDER BY k);

SELECT count(), sum(k) FROM (SELECT k FROM (SELECT k FROM t_sliced_union_one_part UNION ALL SELECT k FROM t_sliced_union_many_parts) ORDER BY k);

DROP TABLE t_sliced_union_one_part;
DROP TABLE t_sliced_union_many_parts;
