-- A sort-merge join chosen by DPsub must not use a class of columns of different types as a merge key:
-- equality of `Date` and `DateTime` in two time zones is not transitive.
-- https://github.com/ClickHouse/ClickHouse/issues/130142

SET enable_analyzer = 1;
SET query_plan_optimize_join_order_algorithm = 'dpsub';
SET query_plan_join_order_use_interesting_orders = 1;
SET enable_join_transitive_predicates = 1;
SET optimize_read_in_order = 1;
SET optimize_sorting_by_input_stream_properties = 1;
SET query_plan_join_shard_by_pk_ranges = 1;
SET max_rows_in_set_to_optimize_join = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 0;
SET use_hash_table_stats_for_join_reordering = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_mixed_a;
DROP TABLE IF EXISTS t_mixed_b;
DROP TABLE IF EXISTS t_mixed_c;

CREATE TABLE t_mixed_a (k UInt64, x Date) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_mixed_b (k UInt64, y DateTime('UTC')) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_mixed_c (k UInt64, z DateTime('Asia/Tokyo')) ENGINE = MergeTree ORDER BY (k, z);

-- For k = 1, `x = y` and `y = z` hold, `x = z` does not: the `Date` is midnight in UTC, not in Tokyo.
-- For k = 2, `x = y` holds, `y = z` does not, so the row must not match.
INSERT INTO t_mixed_a VALUES (1, '2024-01-01'), (2, '2024-01-01');
INSERT INTO t_mixed_b VALUES (1, toDateTime('2024-01-01 00:00:00', 'UTC')), (2, toDateTime('2024-01-01 00:00:00', 'UTC'));
INSERT INTO t_mixed_c VALUES (1, toDateTime('2024-01-01 00:00:00', 'UTC')), (2, toDateTime('2024-01-01 00:00:01', 'UTC'));

-- Joins `t_mixed_a` with `t_mixed_c` first as a sort-merge join on `k`.
SET param__internal_join_table_stat_hints = '{"t_mixed_a": {"cardinality": 20000000, "distinct_keys": {"k": 20000000}}, "t_mixed_b": {"cardinality": 100000000, "distinct_keys": {"k": 100000000}}, "t_mixed_c": {"cardinality": 50000000, "distinct_keys": {"k": 50000000}}}';

SELECT t_mixed_a.k
FROM t_mixed_a
JOIN t_mixed_b ON (t_mixed_a.k = t_mixed_b.k AND t_mixed_a.x = t_mixed_b.y)
JOIN t_mixed_c ON (t_mixed_b.k = t_mixed_c.k AND t_mixed_b.y = t_mixed_c.z)
ORDER BY ALL;

SELECT t_mixed_a.k
FROM t_mixed_a
JOIN t_mixed_b ON (t_mixed_a.k = t_mixed_b.k AND t_mixed_a.x = t_mixed_b.y)
JOIN t_mixed_c ON (t_mixed_b.k = t_mixed_c.k AND t_mixed_b.y = t_mixed_c.z)
ORDER BY ALL
SETTINGS query_plan_join_order_use_interesting_orders = 0;

DROP TABLE t_mixed_a;
DROP TABLE t_mixed_b;
DROP TABLE t_mixed_c;
