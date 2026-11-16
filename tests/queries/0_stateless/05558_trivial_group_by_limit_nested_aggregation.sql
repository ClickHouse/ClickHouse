-- Tags: shard
-- The limit on groups for `GROUP BY ... LIMIT` without aggregate functions must not apply to other aggregations:
-- in the views that the query reads, and in its subqueries that shards execute.

SET optimize_trivial_group_by_limit_query = 1;
-- The limit on the number of groups is checked after each block.
SET max_block_size = 10, max_threads = 1;

DROP VIEW IF EXISTS v;
DROP VIEW IF EXISTS pv;
DROP VIEW IF EXISTS cv;
DROP VIEW IF EXISTS last;
DROP TABLE IF EXISTS t;

CREATE TABLE t (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 10;
INSERT INTO t SELECT number % 100, number FROM numbers(1000);

CREATE VIEW v AS SELECT k, max(x) AS m FROM t GROUP BY k;
CREATE VIEW pv AS SELECT k, max(x) AS m FROM t WHERE x >= {lo:UInt64} GROUP BY k;
CREATE VIEW cv AS SELECT k, (SELECT u.x FROM t AS u WHERE u.k = a.k AND u.x >= 900 GROUP BY u.x LIMIT 1) AS m FROM t AS a;
CREATE VIEW last AS SELECT k FROM t GROUP BY k HAVING max(x) = 999;

SELECT 'view', k FROM v WHERE m = 999 GROUP BY k LIMIT 5;
SELECT 'parameterized view', k FROM pv(lo = 0) WHERE m = 999 GROUP BY k LIMIT 5;
SELECT 'correlated subquery in view', k FROM cv WHERE m = 999 GROUP BY k LIMIT 1;
SELECT 'view in JOIN', t.k FROM t INNER JOIN v ON t.k = v.k WHERE v.m = 999 GROUP BY t.k LIMIT 5;
SELECT 'view in IN', k FROM t WHERE k IN last GROUP BY k LIMIT 5;
SELECT 'function view', k FROM view(SELECT k, max(x) AS m FROM t GROUP BY k) WHERE m = 999 GROUP BY k LIMIT 5;
SELECT 'function merge', k FROM merge(currentDatabase(), '^v$') WHERE m = 999 GROUP BY k LIMIT 5;
SELECT 'subquery', k FROM (SELECT k, max(x) AS m FROM t GROUP BY k) WHERE m = 999 GROUP BY k LIMIT 5;
SELECT 'scalar subquery', (SELECT k FROM v WHERE m = 999 GROUP BY k LIMIT 1);

-- `LIMIT` of a correlated subquery is for each outer row.
SELECT 'correlated aggregate', max(k) FROM v AS a WHERE m = (SELECT max(u.x) FROM t AS u WHERE u.k = a.k GROUP BY u.k LIMIT 1)
SETTINGS enable_group_by_top_k_optimization = 0;
SELECT 'correlated aggregate in EXISTS', max(k) FROM v AS a WHERE EXISTS (SELECT 1 FROM (SELECT max(u.x) AS y FROM t AS u WHERE u.k = a.k GROUP BY u.k LIMIT 1) WHERE y >= 900)
SETTINGS enable_group_by_top_k_optimization = 0;
SELECT 'correlated aggregate in LATERAL', max(a.k) FROM v AS a JOIN LATERAL (SELECT max(u.x) AS y FROM t AS u WHERE u.k = a.k GROUP BY u.k LIMIT 1) AS l ON true WHERE y = m
SETTINGS enable_group_by_top_k_optimization = 0, allow_experimental_lateral_join = 1;
-- No limit on groups if the query can be evaluated twice for a correlated subquery.
SELECT k, (SELECT max(number) FROM numbers(3) WHERE number = a.k) FROM t AS a GROUP BY k LIMIT 5
SETTINGS max_rows_to_group_by = 50, optimize_aggregation_in_order = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0; -- { serverError TOO_MANY_ROWS }

SELECT 'view on shards', k FROM remote('127.0.0.{1,2}', currentDatabase(), v) WHERE m = 999 GROUP BY k LIMIT 5;
SELECT 'IN on shards', k FROM remote('127.0.0.{1,2}', currentDatabase(), t) WHERE k IN (SELECT k FROM t GROUP BY k HAVING max(x) = 999) GROUP BY k LIMIT 5;
SELECT 'JOIN on shards', k FROM remote('127.0.0.{1,2}', currentDatabase(), t) AS l JOIN (SELECT k FROM t GROUP BY k HAVING max(x) = 999) AS r USING k GROUP BY k LIMIT 5;
SELECT 'IN on replicas', k FROM t WHERE k IN (SELECT k FROM t GROUP BY k HAVING max(x) = 999) GROUP BY k LIMIT 5
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;

-- The query cache refuses to work with `group_by_overflow_mode = 'any'`.
SELECT 'query cache', number FROM remote('127.0.0.2', numbers(100)) WHERE number = 99 GROUP BY number LIMIT 5 SETTINGS use_query_cache = 1;

DROP VIEW v;
DROP VIEW pv;
DROP VIEW cv;
DROP VIEW last;
DROP TABLE t;
