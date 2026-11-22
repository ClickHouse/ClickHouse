-- A join on a key that is unique on one side is estimated at no more rows than the other side, also when the
-- `uniq_v2` count of the key is a few percent off the row count. A chain of such joins must not compound that error.

SET materialize_statistics_on_insert = 1;
SET use_statistics = 1;
SET use_hash_table_stats_for_join_reordering = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'pretty';

DROP TABLE IF EXISTS t_u1;
DROP TABLE IF EXISTS t_u2;
DROP TABLE IF EXISTS t_u3;
DROP TABLE IF EXISTS t_o1;
DROP TABLE IF EXISTS t_o2;

CREATE TABLE t_u1 (id UInt64, d UInt64) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = 'uniq_v2';
CREATE TABLE t_u2 (id UInt64, d UInt64) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = 'uniq_v2';
CREATE TABLE t_u3 (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = 'uniq_v2';
CREATE TABLE t_o1 (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = 'uniq_v2';
CREATE TABLE t_o2 (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = 'uniq_v2';

-- `uniq_v2` counts 98160 distinct values in 100000 rows and 40663 in 40000.
INSERT INTO t_u1 SELECT number, intDiv(number, 2) FROM numbers(100000);
INSERT INTO t_u2 SELECT number, intDiv(number, 2) FROM numbers(100000);
INSERT INTO t_u3 SELECT number FROM numbers(100000);
INSERT INTO t_o1 SELECT number FROM numbers(40000);
INSERT INTO t_o2 SELECT number FROM numbers(40000);

SELECT 'chain';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '')
FROM (EXPLAIN SELECT count() FROM t_u1 JOIN t_u2 USING (id) JOIN t_u3 USING (id))
WHERE explain LIKE '%Selectivity:%' OR explain LIKE '%Output rows:%';

SELECT 'chain, greedy';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '')
FROM (EXPLAIN SELECT count() FROM t_u1 JOIN t_u2 USING (id) JOIN t_u3 USING (id) SETTINGS query_plan_optimize_join_order_algorithm = 'greedy')
WHERE explain LIKE '%Selectivity:%' OR explain LIKE '%Output rows:%';

SELECT 'count above the row count';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '')
FROM (EXPLAIN SELECT count() FROM t_o1 JOIN t_o2 USING (id))
WHERE explain LIKE '%Selectivity:%' OR explain LIKE '%Output rows:%';

SELECT 'unique on both sides';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '')
FROM (EXPLAIN SELECT count() FROM t_u1 JOIN t_o1 USING (id))
WHERE explain LIKE '%Selectivity:%' OR explain LIKE '%Output rows:%';

-- Every value of `d` is there twice: not a unique key, so the join keeps about twice the rows.
SELECT 'not unique';
SELECT toFloat64(extract(explain, 'estimated ([0-9.]+) thousand')) BETWEEN 190 AND 210
FROM (EXPLAIN SELECT count() FROM t_u1 JOIN t_u2 ON t_u1.d = t_u2.d)
WHERE explain LIKE '%Output rows:%';

DROP TABLE t_u1;
DROP TABLE t_u2;
DROP TABLE t_u3;
DROP TABLE t_o1;
DROP TABLE t_o2;
