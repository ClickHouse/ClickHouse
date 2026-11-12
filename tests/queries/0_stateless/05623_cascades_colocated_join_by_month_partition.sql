-- Two tables partitioned by the month of a date and joined on the date: with `distributed_plan_use_partition_key_layout`
-- every node joins the months of its bucket, with no exchange below the join. Outer and `ANY` joins and an aggregation
-- above the join must give the results of a join on one node: every row is in one bucket, so an unmatched row is
-- returned one time, and all rows of one key are on one node.

SET explain_query_plan_default = 'legacy';
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_fallback_to_local_execution = 0;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
-- The Fast test profile sets a non-zero max_rows_to_group_by, which keeps aggregations local.
SET max_rows_to_group_by = 0;
SET use_statistics = 0;
-- The test pins full EXPLAIN outputs, so the randomized settings that shape these plans are pinned.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET optimize_move_to_prewhere = 1;
SET query_plan_merge_filters = 1;
SET query_plan_merge_filter_into_join_condition = 1;
SET query_plan_remove_unused_columns = 1;
SET enable_join_transitive_predicates = 1;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_coloc_month_left;
DROP TABLE IF EXISTS t_coloc_month_right;

CREATE TABLE t_coloc_month_left (date Date, id UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY toYYYYMM(date) ORDER BY id SETTINGS auto_statistics_types = '';
CREATE TABLE t_coloc_month_right (date Date, id UInt64, w UInt64)
ENGINE = MergeTree PARTITION BY toYYYYMM(date) ORDER BY id SETTINGS auto_statistics_types = '';

-- A merge between planning and the read of a task would invalidate the planned part names.
SYSTEM STOP MERGES t_coloc_month_left;
SYSTEM STOP MERGES t_coloc_month_right;

-- The left table has months 1 .. 8 and the right table has months 3 .. 10, so each side has months with no match.
INSERT INTO t_coloc_month_left SELECT toDate('2026-01-01') + number % 240, intDiv(number, 240), number FROM numbers(24000);
INSERT INTO t_coloc_month_right SELECT toDate('2026-03-01') + number % 240, intDiv(number, 240), number FROM numbers(2400);

-- The hints claim big tables, so that a plan on 4 nodes is cheaper than a plan on one node.
SET param__internal_join_table_stat_hints = '{"t_coloc_month_left": {"cardinality": 1000000000, "avg_row_bytes": 24, "distinct_keys": {"date": 100000000, "id": 100}}, "t_coloc_month_right": {"cardinality": 100000000, "avg_row_bytes": 24, "distinct_keys": {"date": 100000000, "id": 10}}}';
SET distributed_plan_use_partition_key_layout = 1;

SELECT '-- 1. join on the date and one more column';
EXPLAIN SELECT count(), sum(l.v), sum(r.w) FROM t_coloc_month_left AS l INNER JOIN t_coloc_month_right AS r ON l.date = r.date AND l.id = r.id;
SELECT count(), sum(l.v), sum(r.w) FROM t_coloc_month_left AS l INNER JOIN t_coloc_month_right AS r ON l.date = r.date AND l.id = r.id;
SELECT count(), sum(l.v), sum(r.w) FROM t_coloc_month_left AS l INNER JOIN t_coloc_month_right AS r ON l.date = r.date AND l.id = r.id SETTINGS make_distributed_plan = 0;

SELECT '-- 2. join on the date only';
SELECT count(), sum(l.v), sum(r.w) FROM t_coloc_month_left AS l INNER JOIN t_coloc_month_right AS r ON l.date = r.date;
SELECT count(), sum(l.v), sum(r.w) FROM t_coloc_month_left AS l INNER JOIN t_coloc_month_right AS r ON l.date = r.date SETTINGS make_distributed_plan = 0;

SELECT '-- 3. LEFT, RIGHT and FULL join: rows with no match are returned one time';
SELECT count(), countIf(r.date = '1970-01-01'), sum(l.v), sum(r.w) FROM t_coloc_month_left AS l LEFT JOIN t_coloc_month_right AS r ON l.date = r.date AND l.id = r.id;
SELECT count(), countIf(r.date = '1970-01-01'), sum(l.v), sum(r.w) FROM t_coloc_month_left AS l LEFT JOIN t_coloc_month_right AS r ON l.date = r.date AND l.id = r.id SETTINGS make_distributed_plan = 0;
SELECT count(), countIf(l.date = '1970-01-01'), sum(l.v), sum(r.w) FROM t_coloc_month_left AS l RIGHT JOIN t_coloc_month_right AS r ON l.date = r.date AND l.id = r.id;
SELECT count(), countIf(l.date = '1970-01-01'), sum(l.v), sum(r.w) FROM t_coloc_month_left AS l RIGHT JOIN t_coloc_month_right AS r ON l.date = r.date AND l.id = r.id SETTINGS make_distributed_plan = 0;
EXPLAIN SELECT count(), countIf(l.date = '1970-01-01'), countIf(r.date = '1970-01-01') FROM t_coloc_month_left AS l FULL JOIN t_coloc_month_right AS r ON l.date = r.date AND l.id = r.id;
SELECT count(), countIf(l.date = '1970-01-01'), countIf(r.date = '1970-01-01') FROM t_coloc_month_left AS l FULL JOIN t_coloc_month_right AS r ON l.date = r.date AND l.id = r.id;
SELECT count(), countIf(l.date = '1970-01-01'), countIf(r.date = '1970-01-01') FROM t_coloc_month_left AS l FULL JOIN t_coloc_month_right AS r ON l.date = r.date AND l.id = r.id SETTINGS make_distributed_plan = 0;

SELECT '-- 4. ANY join: one row for each key, because all rows of a key are on one node';
EXPLAIN SELECT count() FROM t_coloc_month_left AS l ANY INNER JOIN t_coloc_month_right AS r ON l.date = r.date;
SELECT count() FROM t_coloc_month_left AS l ANY INNER JOIN t_coloc_month_right AS r ON l.date = r.date;
SELECT count() FROM t_coloc_month_left AS l ANY INNER JOIN t_coloc_month_right AS r ON l.date = r.date SETTINGS make_distributed_plan = 0;

SELECT '-- 5. GROUP BY a right column above a LEFT join: the rows with no match are one group';
SELECT count(), sum(c), max(c) FROM (SELECT r.date AS d, count() AS c FROM t_coloc_month_left AS l LEFT JOIN t_coloc_month_right AS r ON l.date = r.date AND l.id = r.id GROUP BY d);
SELECT count(), sum(c), max(c) FROM (SELECT r.date AS d, count() AS c FROM t_coloc_month_left AS l LEFT JOIN t_coloc_month_right AS r ON l.date = r.date AND l.id = r.id GROUP BY d) SETTINGS make_distributed_plan = 0;

DROP TABLE t_coloc_month_left;
DROP TABLE t_coloc_month_right;
