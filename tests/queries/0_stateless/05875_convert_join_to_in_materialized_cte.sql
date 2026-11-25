-- Tags: no-parallel-replicas
-- no-parallel-replicas: a JOIN on a MATERIALIZED CTE over-counts under parallel replicas, a separate defect

-- The IN set that query_plan_convert_join_to_in makes from a right side reading a MATERIALIZED CTE
-- was built for index analysis before the CTE was materialized.

SET enable_materialized_cte = 1;
SET query_plan_convert_join_to_in = 1;
SET use_index_for_in_with_subqueries = 1;

DROP TABLE IF EXISTS t_join_to_in_cte;
DROP TABLE IF EXISTS t_join_to_in_cte_bf;
DROP TABLE IF EXISTS t_join_to_in_cte_2;

CREATE TABLE t_join_to_in_cte (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_join_to_in_cte SELECT number FROM numbers(10000);

-- Primary key analysis.
WITH s AS MATERIALIZED (SELECT number AS k FROM numbers(10))
SELECT count() FROM t_join_to_in_cte AS a INNER JOIN s ON a.k = s.k WHERE a.k IN (SELECT k FROM s);

-- The rewrite is kept.
SELECT count() > 0 FROM (
    EXPLAIN WITH s AS MATERIALIZED (SELECT number AS k FROM numbers(10))
    SELECT count() FROM t_join_to_in_cte AS a INNER JOIN s ON a.k = s.k WHERE a.k IN (SELECT k FROM s)
) WHERE explain ILIKE '%Calculate join left keys%';

-- The CTE is read only by joins.
WITH s AS MATERIALIZED (SELECT number AS k FROM numbers(10))
SELECT count() FROM t_join_to_in_cte AS a INNER JOIN s ON a.k = s.k INNER JOIN s AS s2 ON a.k = s2.k;

-- The CTE is read by an IN subquery of the right side.
WITH s AS MATERIALIZED (SELECT number AS k FROM numbers(10))
SELECT count() FROM t_join_to_in_cte AS a
INNER JOIN (SELECT number AS k FROM numbers(100) WHERE number IN (SELECT k FROM s)) AS r ON a.k = r.k
WHERE a.k IN (SELECT k FROM s);

-- The joined CTE reads another CTE.
WITH
    s0 AS MATERIALIZED (SELECT number AS k FROM numbers(10)),
    s1 AS MATERIALIZED (SELECT k FROM s0 WHERE k < 5 UNION ALL SELECT k FROM s0 WHERE k >= 5)
SELECT count() FROM t_join_to_in_cte AS a INNER JOIN s1 ON a.k = s1.k INNER JOIN s1 AS s1b ON a.k = s1b.k;

-- The join is in the definition of another CTE.
WITH
    s0 AS MATERIALIZED (SELECT number AS k FROM numbers(10)),
    s AS MATERIALIZED (SELECT count() AS c FROM t_join_to_in_cte AS a INNER JOIN s0 ON a.k = s0.k WHERE a.k IN (SELECT k FROM s0))
SELECT * FROM s UNION ALL SELECT * FROM s;

-- Skip index analysis of a Merge table child.
CREATE TABLE t_join_to_in_cte_bf (k UInt64, INDEX bf k TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 128;
INSERT INTO t_join_to_in_cte_bf SELECT number FROM numbers(10000);

WITH s AS MATERIALIZED (SELECT number AS k FROM numbers(10))
SELECT count() FROM merge(currentDatabase(), '^t_join_to_in_cte_bf$') AS a INNER JOIN s ON a.k = s.k WHERE a.k IN (SELECT k FROM s);

-- A Merge table on the right side, filtered by a set reading the CTE.
CREATE TABLE t_join_to_in_cte_2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_join_to_in_cte_2 SELECT number, number FROM numbers(1000);

WITH s AS MATERIALIZED (SELECT number AS k FROM numbers(10))
SELECT count() FROM t_join_to_in_cte AS a
INNER JOIN (SELECT * FROM merge(currentDatabase(), '^t_join_to_in_cte_2$') WHERE v IN (SELECT k FROM s)) AS r ON a.k = r.k
WHERE a.k IN (SELECT k FROM s)
SETTINGS optimize_move_to_prewhere = 0;

-- A remote read on the right side, filtered by a set reading the CTE.
WITH s AS MATERIALIZED (SELECT number AS k FROM numbers(10))
SELECT count() FROM t_join_to_in_cte AS a
INNER JOIN (SELECT k FROM remote('127.0.0.2', currentDatabase(), t_join_to_in_cte_2) WHERE v IN (SELECT k FROM s)) AS r ON a.k = r.k
WHERE a.k IN (SELECT k FROM s);

-- The rewrite is kept.
SELECT count() > 0 FROM (
    EXPLAIN WITH s AS MATERIALIZED (SELECT number AS k FROM numbers(10))
    SELECT count() FROM t_join_to_in_cte AS a
    INNER JOIN (SELECT k FROM remote('127.0.0.2', currentDatabase(), t_join_to_in_cte_2) WHERE v IN (SELECT k FROM s)) AS r ON a.k = r.k
    WHERE a.k IN (SELECT k FROM s)
) WHERE explain ILIKE '%Calculate join left keys%';

-- Parallel replicas without a local plan on the right side.
WITH s AS MATERIALIZED (SELECT number AS k FROM numbers(10))
SELECT count() FROM merge(currentDatabase(), '^t_join_to_in_cte_bf$') AS a
INNER JOIN (SELECT k FROM t_join_to_in_cte_2 WHERE v IN (SELECT k FROM s)) AS r ON a.k = r.k
WHERE a.k IN (SELECT k FROM s)
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_for_queries_with_multiple_tables = 1, parallel_replicas_local_plan = 0,
    automatic_parallel_replicas_mode = 0;

-- The rewrite is kept.
SELECT countIf(explain ILIKE '%Join (JOIN%') = 0 AND countIf(explain ILIKE '%ReadFromMerge%') > 0 FROM (
    EXPLAIN WITH s AS MATERIALIZED (SELECT number AS k FROM numbers(10))
    SELECT count() FROM merge(currentDatabase(), '^t_join_to_in_cte_bf$') AS a
    INNER JOIN (SELECT k FROM t_join_to_in_cte_2 WHERE v IN (SELECT k FROM s)) AS r ON a.k = r.k
    WHERE a.k IN (SELECT k FROM s)
    SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'parallel_replicas',
        parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_for_queries_with_multiple_tables = 1, parallel_replicas_local_plan = 0,
        automatic_parallel_replicas_mode = 0
);

DROP TABLE t_join_to_in_cte;
DROP TABLE t_join_to_in_cte_bf;
DROP TABLE t_join_to_in_cte_2;
