-- An n-way join whose side is shipped to the replicas as a GLOBAL temporary table must not be read once per replica.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;
DROP VIEW IF EXISTS v;

CREATE TABLE t1 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t1 SELECT number FROM numbers(100);
CREATE TABLE t2 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t2 SELECT number FROM numbers(100);
CREATE VIEW v AS SELECT k FROM t2;
CREATE TEMPORARY TABLE tmp (k UInt64) ENGINE = Memory;
INSERT INTO tmp SELECT number FROM numbers(100);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_plan_based = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_for_queries_with_multiple_tables = 1;

SELECT 'temporary table';
SELECT count(), sum(t1.k) FROM t1 INNER JOIN tmp ON t1.k = tmp.k INNER JOIN t2 ON t1.k = t2.k
    SETTINGS parallel_replicas_local_plan = 0;
SELECT count(), sum(t1.k) FROM t1 INNER JOIN tmp ON t1.k = tmp.k INNER JOIN t2 ON t1.k = t2.k
    SETTINGS parallel_replicas_prefer_local_replica = 0;

SELECT 'view';
SELECT count() FROM t1 INNER JOIN v USING (k) LEFT JOIN t2 USING (k)
    SETTINGS parallel_replicas_local_plan = 0;
SELECT count() FROM t1 INNER JOIN v USING (k) LEFT JOIN t2 USING (k)
    SETTINGS parallel_replicas_prefer_local_replica = 0;

SELECT 'parallel_replicas_prefer_local_join = 0';
SELECT count() FROM t1 INNER JOIN t2 USING (k) LEFT JOIN t2 AS t3 USING (k)
    SETTINGS parallel_replicas_prefer_local_join = 0, parallel_replicas_local_plan = 0;
SELECT count() FROM t1 INNER JOIN t2 USING (k) LEFT JOIN t2 AS t3 USING (k)
    SETTINGS parallel_replicas_prefer_local_join = 0, parallel_replicas_prefer_local_replica = 0;

SELECT 'materialized CTE';
WITH s AS MATERIALIZED (SELECT number AS k FROM numbers(10))
SELECT count() FROM t1 INNER JOIN s ON t1.k = s.k INNER JOIN s AS s2 ON t1.k = s2.k
    SETTINGS enable_materialized_cte = 1, parallel_replicas_local_plan = 0;
WITH s AS MATERIALIZED (SELECT number AS k FROM numbers(10))
SELECT count() FROM t1 INNER JOIN s ON t1.k = s.k INNER JOIN s AS s2 ON t1.k = s2.k
    SETTINGS enable_materialized_cte = 1, parallel_replicas_prefer_local_replica = 0;

SELECT 'join in a subquery';
SELECT count() FROM (SELECT t1.k FROM t1 INNER JOIN tmp ON t1.k = tmp.k INNER JOIN t2 ON t1.k = t2.k)
    SETTINGS parallel_replicas_local_plan = 0;
SELECT count() FROM (SELECT t1.k FROM t1 INNER JOIN tmp ON t1.k = tmp.k INNER JOIN t2 ON t1.k = t2.k)
    SETTINGS parallel_replicas_prefer_local_replica = 0;

DROP VIEW v;
DROP TABLE t1;
DROP TABLE t2;
