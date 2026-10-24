-- Lazy materialization over joins is not used where a source can produce a block of totals - `WITH TOTALS`, or a remote
-- read, whose query can have it: the block goes through the joins up to the LIMIT with no rows to read lazily, and a
-- value computed below a join is that join's default in it. The results are compared with the optimization disabled.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO l SELECT number, concat('p', toString(number)) FROM numbers(10);
INSERT INTO r SELECT number % 5, number FROM numbers(20);

SELECT '-- WITH TOTALS on the right side';
SELECT l.k, l.payload, x.c FROM l LEFT JOIN (SELECT k, count() AS c FROM r GROUP BY k WITH TOTALS) AS x ON l.k = x.k
ORDER BY l.k LIMIT 2 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.payload, x.c FROM l LEFT JOIN (SELECT k, count() AS c FROM r GROUP BY k WITH TOTALS) AS x ON l.k = x.k
ORDER BY l.k LIMIT 2 SETTINGS query_plan_lazy_materialization_for_join = 0;

SELECT '-- WITH TOTALS on the left side';
SELECT x.k, x.c, l.payload FROM (SELECT k, count() AS c FROM r GROUP BY k WITH TOTALS) AS x JOIN l ON x.k = l.k
ORDER BY x.k LIMIT 2 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT x.k, x.c, l.payload FROM (SELECT k, count() AS c FROM r GROUP BY k WITH TOTALS) AS x JOIN l ON x.k = l.k
ORDER BY x.k LIMIT 2 SETTINGS query_plan_lazy_materialization_for_join = 0;

-- `remote` reads the local replica locally where it can, so whether the plan has a remote read depends on the settings.
SELECT '-- a remote read';
SELECT l.k, l.payload, x.c FROM l LEFT JOIN (SELECT k, count() AS c FROM remote('127.0.0.1', currentDatabase(), r) GROUP BY k WITH TOTALS) AS x
ON l.k = x.k ORDER BY l.k LIMIT 2 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.payload, x.c FROM l LEFT JOIN (SELECT k, count() AS c FROM remote('127.0.0.1', currentDatabase(), r) GROUP BY k WITH TOTALS) AS x
ON l.k = x.k ORDER BY l.k LIMIT 2 SETTINGS query_plan_lazy_materialization_for_join = 0;

SELECT '-- nothing is read lazily';
SELECT countIf(explain LIKE '%LazilyRead%') FROM (
    EXPLAIN SELECT l.k, l.payload, x.c FROM l LEFT JOIN (SELECT k, count() AS c FROM r GROUP BY k WITH TOTALS) AS x ON l.k = x.k ORDER BY l.k LIMIT 2);

DROP TABLE l;
DROP TABLE r;
