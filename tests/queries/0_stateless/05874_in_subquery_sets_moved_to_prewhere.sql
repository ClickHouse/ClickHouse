-- Sets of `IN (subquery)` conditions moved to PREWHERE are built when the query runs, not during planning.
-- https://github.com/ClickHouse/ClickHouse/issues/134090

SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET move_all_conditions_to_prewhere = 1;

DROP TABLE IF EXISTS t_fact;
DROP TABLE IF EXISTS t_fact_memory;
DROP TABLE IF EXISTS t_dim;
DROP TABLE IF EXISTS t_log;
DROP TABLE IF EXISTS t_mt;
DROP TABLE IF EXISTS t_mt2;

CREATE TABLE t_fact (id UInt64, a UInt32, b UInt32, v Float64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_fact_memory (id UInt64, a UInt32, b UInt32, v Float64) ENGINE = Memory;
CREATE TABLE t_dim (id UInt32, name String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_fact SELECT number, number % 300, (number * 7) % 300, number % 10 FROM numbers(5000);
INSERT INTO t_fact_memory SELECT * FROM t_fact;
INSERT INTO t_dim SELECT number, concat('name_', toString(number)) FROM numbers(300);

-- The query gives the same result with and without the move to PREWHERE.
SELECT count(), sum(v) FROM t_fact WHERE a IN (SELECT id FROM t_dim WHERE name LIKE '%1%') AND b IN (SELECT id FROM t_dim WHERE name LIKE '%2%');
SELECT count(), sum(v) FROM t_fact WHERE a IN (SELECT id FROM t_dim WHERE name LIKE '%1%') AND b IN (SELECT id FROM t_dim WHERE name LIKE '%2%') SETTINGS optimize_move_to_prewhere = 0;
SELECT count(), sum(v) FROM t_fact_memory WHERE a IN (SELECT id FROM t_dim WHERE name LIKE '%1%') AND b IN (SELECT id FROM t_dim WHERE name LIKE '%2%');
SELECT count(), sum(v) FROM t_fact_memory WHERE a IN (SELECT id FROM t_dim WHERE name LIKE '%1%') AND b IN (SELECT id FROM t_dim WHERE name LIKE '%2%') SETTINGS optimize_move_to_prewhere = 0;

-- A plain EXPLAIN does not run the subqueries.
SELECT count() > 0 FROM (EXPLAIN SELECT count(), sum(v) FROM t_fact WHERE a IN (SELECT id FROM t_dim WHERE name LIKE '%1%') AND b IN (SELECT id FROM t_dim WHERE name LIKE '%2%')) SETTINGS log_comment = '05874_explain_mt', parallel_replicas_plan_based = 0;
SELECT count() > 0 FROM (EXPLAIN SELECT count(), sum(v) FROM t_fact_memory WHERE a IN (SELECT id FROM t_dim WHERE name LIKE '%1%') AND b IN (SELECT id FROM t_dim WHERE name LIKE '%2%')) SETTINGS log_comment = '05874_explain_memory';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['SetsBuiltFromSubquery']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND log_comment IN ('05874_explain_mt', '05874_explain_memory')
ORDER BY log_comment;

-- A teardown from above must not start a read whose PREWHERE set is not built yet.
CREATE TABLE t_log (v0 UInt32) ENGINE = Log;
CREATE TABLE t_mt (v0 UInt32, v1 UInt32, v2 DateTime) ENGINE = MergeTree ORDER BY v1;
CREATE TABLE t_mt2 (v0 UInt32, v1 UInt32) ENGINE = MergeTree ORDER BY v0;
INSERT INTO t_mt VALUES (13, 23000, '2100-01-05');
INSERT INTO t_mt VALUES (16, 26000, '2066-10-07');
INSERT INTO t_mt2 VALUES (91, 101000);

SELECT * FROM (SELECT 1 UNION ALL SELECT r.v0 FROM (SELECT row_number() OVER (PARTITION BY t.v0) AS c1 FROM t_mt AS t WHERE t.v2 IN (SELECT 1 FROM t_log)) AS l INNER JOIN t_mt2 AS r ON l.c1 = r.v0) LIMIT 1 SETTINGS max_threads = 4;
-- The radix teardown depends on scheduling, so it runs several times.
SELECT * FROM (SELECT 1 UNION ALL SELECT uniqExact(t.v1) + 1 FROM t_mt AS t WHERE t.v2 IN (SELECT 1 FROM t_log)) LIMIT 1 SETTINGS max_threads = 2, optimize_uniq_exact_radix_partitioning = 1;
SELECT * FROM (SELECT 1 UNION ALL SELECT uniqExact(t.v1) + 1 FROM t_mt AS t WHERE t.v2 IN (SELECT 1 FROM t_log)) LIMIT 1 SETTINGS max_threads = 2, optimize_uniq_exact_radix_partitioning = 1;
SELECT * FROM (SELECT 1 UNION ALL SELECT uniqExact(t.v1) + 1 FROM t_mt AS t WHERE t.v2 IN (SELECT 1 FROM t_log)) LIMIT 1 SETTINGS max_threads = 2, optimize_uniq_exact_radix_partitioning = 1;
SELECT * FROM (SELECT 1 UNION ALL SELECT uniqExact(t.v1) + 1 FROM t_mt AS t WHERE t.v2 IN (SELECT 1 FROM t_log)) LIMIT 1 SETTINGS max_threads = 2, optimize_uniq_exact_radix_partitioning = 1;
SELECT * FROM (SELECT 1 UNION ALL SELECT uniqExact(t.v1) + 1 FROM t_mt AS t WHERE t.v2 IN (SELECT 1 FROM t_log)) LIMIT 1 SETTINGS max_threads = 2, optimize_uniq_exact_radix_partitioning = 1;

DROP TABLE t_fact;
DROP TABLE t_fact_memory;
DROP TABLE t_dim;
DROP TABLE t_log;
DROP TABLE t_mt;
DROP TABLE t_mt2;
