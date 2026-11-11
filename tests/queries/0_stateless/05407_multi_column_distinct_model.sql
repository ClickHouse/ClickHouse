-- The settings `query_plan_group_multi_key_model` and `query_plan_join_multi_key_model` select how the distinct
-- combinations of several key columns follow from the NDVs of the columns, for experiments that compare the models. The three columns are
-- independent with 100, 50 and 10 values in 100,000 rows, so the true number of combinations is 50,000.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET param__internal_cascades_cluster_node_count = 2;
SET max_rows_to_group_by = 0;
SET explain_query_plan_default = 'legacy';
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_mcd;
CREATE TABLE t_mcd (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a SETTINGS auto_statistics_types = 'uniq';
INSERT INTO t_mcd SELECT number % 100, intDiv(number, 100) % 50, intDiv(number, 5000) % 10 FROM numbers(100000);

-- The row estimate of the aggregation under each model.
SELECT '-- default and largest: the largest NDV, 100';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT a, b, c, count() FROM t_mcd GROUP BY a, b, c) WHERE trimLeft(explain) LIKE 'Aggregating%';
SET query_plan_group_multi_key_model = 'largest';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT a, b, c, count() FROM t_mcd GROUP BY a, b, c) WHERE trimLeft(explain) LIKE 'Aggregating%';
SELECT '-- product: 100 * 50 * 10';
SET query_plan_group_multi_key_model = 'product';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT a, b, c, count() FROM t_mcd GROUP BY a, b, c) WHERE trimLeft(explain) LIKE 'Aggregating%';
SELECT '-- geometric_mean: sqrt(100 * 50000)';
SET query_plan_group_multi_key_model = 'geometric_mean';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT a, b, c, count() FROM t_mcd GROUP BY a, b, c) WHERE trimLeft(explain) LIKE 'Aggregating%';
SELECT '-- backoff: 100 * 50^(1/2) * 10^(1/4)';
SET query_plan_group_multi_key_model = 'backoff';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT a, b, c, count() FROM t_mcd GROUP BY a, b, c) WHERE trimLeft(explain) LIKE 'Aggregating%';
SELECT '-- per_table: the three columns are of one table, so the largest NDV, 100';
SET query_plan_group_multi_key_model = 'per_table';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT a, b, c, count() FROM t_mcd GROUP BY a, b, c) WHERE trimLeft(explain) LIKE 'Aggregating%';
SELECT '-- postgres: the product 50000, at most 10% of the 100000 rows of the table';
SET query_plan_group_multi_key_model = 'postgres';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT a, b, c, count() FROM t_mcd GROUP BY a, b, c) WHERE trimLeft(explain) LIKE 'Aggregating%';
SELECT '-- postgres: two keys of 100 and 50 values, below the cap, multiply';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT a, b, count() FROM t_mcd GROUP BY a, b) WHERE trimLeft(explain) LIKE 'Aggregating%';

SELECT '-- a join on two key columns of 100 and 50 values (the estimates not randomized): default takes the geometric mean of 1/100 and 1/5000, postgres multiplies 1/100 and 1/50';
SET query_plan_join_multi_key_model = 'default';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT count() FROM t_mcd AS l JOIN t_mcd AS r ON l.a = r.a AND l.b = r.b SETTINGS query_plan_optimize_join_order_randomize = 0) WHERE trimLeft(explain) LIKE 'Join%';
SET query_plan_join_multi_key_model = 'postgres';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT count() FROM t_mcd AS l JOIN t_mcd AS r ON l.a = r.a AND l.b = r.b SETTINGS query_plan_optimize_join_order_randomize = 0) WHERE trimLeft(explain) LIKE 'Join%';

SET query_plan_group_multi_key_model = 'backoff';
SELECT '-- DISTINCT gets the same estimate';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT DISTINCT a, b, c FROM t_mcd) WHERE trimLeft(explain) LIKE 'Distinct%' AND explain NOT LIKE '%Preliminary%';

SELECT '-- the settings are separate: the group count keeps backoff while the join uses postgres';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT a, b, c, count() FROM t_mcd GROUP BY a, b, c) WHERE trimLeft(explain) LIKE 'Aggregating%';
SELECT extract(explain, 'rows: [^,]+') FROM (EXPLAIN estimates = 1 SELECT count() FROM t_mcd AS l JOIN t_mcd AS r ON l.a = r.a AND l.b = r.b SETTINGS query_plan_optimize_join_order_randomize = 0) WHERE trimLeft(explain) LIKE 'Join%';

SELECT '-- an unknown model is an error';
SET query_plan_group_multi_key_model = 'median'; -- { serverError BAD_ARGUMENTS }
SET query_plan_join_multi_key_model = 'median'; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_mcd;
