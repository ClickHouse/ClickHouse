-- Tags: no-parallel-replicas
-- The sliced pool is used for local reading only.

-- The probe side of a hash join read in order through the sliced pool is not gated on the seal of the
-- build side (see enable_join_seal_gated_reading): the pool's sources are plain, and the read keeps the
-- read-time index analysis of the join's runtime filter instead. Without the pool the read is gated.

SET enable_join_runtime_filters = 1;
SET enable_join_seal_gated_reading = 1;
SET enable_parallel_replicas = 0;
SET optimize_read_in_order = 1;
SET query_plan_read_in_order_through_join = 1;
SET read_in_order_use_virtual_row = 1;
SET read_in_order_use_sliced_pool = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';

DROP TABLE IF EXISTS t_sliced_seal_probe;
DROP TABLE IF EXISTS t_sliced_seal_build;

CREATE TABLE t_sliced_seal_probe (k UInt64, v String) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8, auto_statistics_types = '';
INSERT INTO t_sliced_seal_probe SELECT number, toString(number) FROM numbers(50000);

CREATE TABLE t_sliced_seal_build (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_sliced_seal_build SELECT number * 10000 FROM numbers(5);

SELECT p.k FROM t_sliced_seal_probe AS p JOIN t_sliced_seal_build AS b ON p.k = b.k ORDER BY p.k LIMIT 3;
SELECT p.k FROM t_sliced_seal_probe AS p JOIN t_sliced_seal_build AS b ON p.k = b.k ORDER BY p.k DESC LIMIT 3;

SELECT 'sliced pool: router', countIf(explain LIKE '%MergeTreeInOrderSliceRouter%') > 0, 'gated reads', countIf(explain LIKE '%SealGatedRead%')
FROM (EXPLAIN PIPELINE SELECT p.k FROM t_sliced_seal_probe AS p JOIN t_sliced_seal_build AS b ON p.k = b.k ORDER BY p.k LIMIT 3);

SELECT 'per-part reading: router', countIf(explain LIKE '%MergeTreeInOrderSliceRouter%'), 'gated reads', countIf(explain LIKE '%SealGatedRead%') > 0
FROM (EXPLAIN PIPELINE SELECT p.k FROM t_sliced_seal_probe AS p JOIN t_sliced_seal_build AS b ON p.k = b.k ORDER BY p.k LIMIT 3
    SETTINGS read_in_order_use_sliced_pool = 0);

DROP TABLE t_sliced_seal_probe;
DROP TABLE t_sliced_seal_build;
