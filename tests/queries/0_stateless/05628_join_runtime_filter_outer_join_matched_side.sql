-- A join runtime filter may pass a whole block unchecked. It must not be pushed to the side of an OUTER
-- or ANTI join that is only matched against: a match removed there makes the join emit a row it would not.

DROP TABLE IF EXISTS t_rf_left;
DROP TABLE IF EXISTS t_rf_right;
DROP TABLE IF EXISTS t_rf_str;

CREATE TABLE t_rf_left (k UInt32, s Decimal(19, 4)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
CREATE TABLE t_rf_right (k UInt32, x Decimal(19, 4)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_rf_str (k UInt32, v String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;

INSERT INTO t_rf_left SELECT intDiv(number, 20000), 1 FROM numbers(100000);
INSERT INTO t_rf_right SELECT number, 1 FROM numbers(5);
INSERT INTO t_rf_str SELECT intDiv(number, 20000), if(intDiv(number, 20000) = 2, '7', 'x') FROM numbers(100000);

SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, enable_join_runtime_filters_index_analysis = 0;
SET query_plan_optimize_join_order_limit = 1, query_plan_optimize_join_order_randomize = 0, enable_parallel_replicas = 0;
SET join_use_nulls = 0, max_threads = 1, max_block_size = 8192;

-- Every left row has a match, so nothing is divided by zero.
SELECT count(), sum(v.q)
FROM (SELECT l.k AS k, divideDecimal(l.s, r.x) AS q FROM t_rf_left AS l LEFT JOIN t_rf_right AS r ON l.k = r.k) AS v
INNER JOIN (SELECT toUInt32(2) AS k) AS n ON v.k = n.k;

SELECT count(), sum(v.q)
FROM (SELECT l.k AS k, divideDecimal(l.s, r.x) AS q FROM t_rf_right AS r RIGHT JOIN t_rf_left AS l ON r.k = l.k) AS v
INNER JOIN (SELECT toUInt32(2) AS k) AS n ON v.k = n.k;

-- The ANTI JOIN keeps only the rows whose `v` is a number.
SELECT count(), sum(v.u)
FROM (SELECT l.k AS k, toUInt64(l.v) AS u FROM t_rf_str AS l LEFT ANTI JOIN (SELECT k FROM t_rf_right WHERE k != 2) AS b ON l.k = b.k) AS v
INNER JOIN (SELECT toUInt32(2) AS k) AS n ON v.k = n.k;

-- The filter still reaches both sides of an INNER JOIN and the preserved side of a LEFT JOIN.
SELECT countIf(explain LIKE '%RF1(k, 2)%')
FROM (EXPLAIN SELECT sum(v.x) FROM (SELECT l.k AS k, r.x AS x FROM t_rf_left AS l INNER JOIN t_rf_right AS r ON l.k = r.k) AS v INNER JOIN (SELECT toUInt32(2) AS k) AS n ON v.k = n.k);

SELECT countIf(explain LIKE '%RF1(k, 2)%')
FROM (EXPLAIN SELECT sum(v.x) FROM (SELECT l.k AS k, r.x AS x FROM t_rf_left AS l LEFT JOIN t_rf_right AS r ON l.k = r.k) AS v INNER JOIN (SELECT toUInt32(2) AS k) AS n ON v.k = n.k);

DROP TABLE t_rf_left;
DROP TABLE t_rf_right;
DROP TABLE t_rf_str;
