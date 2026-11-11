-- Tags: no-parallel-replicas
-- Random settings limits: query_plan_optimize_join_order_randomize=(0, 0); query_plan_optimize_join_order_limit=(1, None); use_statistics=(1, None); materialize_statistics_on_insert=(1, None); use_hash_table_stats_for_join_reordering=(0, 0)

-- A range condition on a column with `tdigest` statistics is estimated from the statistics of all parts, whatever the order of the parts.

CREATE TABLE t_low_first (v UInt64 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE t_high_first (v UInt64 STATISTICS(tdigest)) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE t_dim (k UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
SYSTEM STOP MERGES t_low_first;
SYSTEM STOP MERGES t_high_first;

INSERT INTO t_low_first SELECT number FROM numbers(5000);
INSERT INTO t_low_first SELECT number + 5000 FROM numbers(5000);
INSERT INTO t_high_first SELECT number + 5000 FROM numbers(5000);
INSERT INTO t_high_first SELECT number FROM numbers(5000);
INSERT INTO t_dim SELECT number FROM numbers(10);

-- Both tables have two parts with statistics on `v`.
SELECT table, count() FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('t_low_first', 't_high_first') AND active AND column = 'v' AND notEmpty(statistics)
GROUP BY table ORDER BY table;

-- 2500 of the 10000 rows satisfy `v < 2500`.
SELECT 'low first', toUInt64OrZero(extract(explain, 't_low_first\\[(\\d+)\\]')) BETWEEN 2250 AND 2750
FROM (EXPLAIN PLAN keep_logical_steps = 1 SELECT count() FROM t_low_first INNER JOIN t_dim ON t_low_first.v = t_dim.k WHERE t_low_first.v < 2500)
WHERE explain LIKE '%Join:%' LIMIT 1;
SELECT 'high first', toUInt64OrZero(extract(explain, 't_high_first\\[(\\d+)\\]')) BETWEEN 2250 AND 2750
FROM (EXPLAIN PLAN keep_logical_steps = 1 SELECT count() FROM t_high_first INNER JOIN t_dim ON t_high_first.v = t_dim.k WHERE t_high_first.v < 2500)
WHERE explain LIKE '%Join:%' LIMIT 1;

DROP TABLE t_low_first;
DROP TABLE t_high_first;
DROP TABLE t_dim;
