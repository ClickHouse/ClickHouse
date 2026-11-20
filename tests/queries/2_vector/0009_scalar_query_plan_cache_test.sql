-- Tags: no-parallel
-- no-parallel: the query plan cache and its entries metric are global
--
-- The query plan cache is not limited to vector search: any SELECT that can be bound to a
-- normalized template with at least one replaceable literal can cache its QueryPlan. This test
-- covers the scalar shapes that are expected to be cached (filter, GROUP BY, ORDER BY + LIMIT,
-- full sort, JOIN with a WHERE literal, HAVING, window function, DISTINCT) and the shapes that
-- are deliberately not cached (no bindable literal, literal only in a JOIN ON clause, subquery,
-- WITH, system table, unsupported function, UNION ALL, tuple literal).
--
-- Each case runs the same SQL twice with the same `log_comment`, so the second execution hits if
-- and only if the first one published a plan. The first execution of a cacheable case is neither
-- a hit nor a miss in `VectorQueryPlanCacheHits/Misses`: those counters are updated only when an
-- AST entry exists and the plan lookup is attempted. `VectorASTCacheHits` shows whether at least
-- the normalized AST was reused for the non-cacheable shapes.

DROP TABLE IF EXISTS scalar_pc_run_state;
DROP TABLE IF EXISTS scalar_pc;
DROP TABLE IF EXISTS scalar_pc_join;
DROP TABLE IF EXISTS scalar_pc_mem;

SET use_query_cache = 0;
SET vector_query_plan_cache = 1;
SET vector_only_cache_query_plan = 0;
SET vector_query_plan_cache_only_vector = 0;

-- Run-scoped start time: assertions only count queries logged after it.
CREATE TABLE scalar_pc_run_state (started DateTime64(6)) ENGINE = Memory;
INSERT INTO scalar_pc_run_state SELECT now64(6);

CREATE TABLE scalar_pc (id UInt32, k UInt32, v UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO scalar_pc SELECT number, number % 10, number FROM numbers(1000);

CREATE TABLE scalar_pc_join (id UInt32, v UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO scalar_pc_join SELECT number, number FROM numbers(1000);

CREATE TABLE scalar_pc_mem (id UInt32, v UInt32) ENGINE = Memory;
INSERT INTO scalar_pc_mem SELECT number, number FROM numbers(100);

SYSTEM DROP VECTOR QUERY PLAN CACHE;

-- C1: one literal in WHERE.
SET log_comment = 'scalar_pc_c1';
SELECT count() FROM scalar_pc WHERE v > 10;
SELECT count() FROM scalar_pc WHERE v > 10;

-- C2: two literals and GROUP BY.
SET log_comment = 'scalar_pc_c2';
SELECT k, sum(v) FROM scalar_pc WHERE v > 20 AND k < 5 GROUP BY k ORDER BY k;
SELECT k, sum(v) FROM scalar_pc WHERE v > 20 AND k < 5 GROUP BY k ORDER BY k;

-- C3: ORDER BY the table key plus LIMIT (read-in-order: FinishSorting/MergingSorted).
SET log_comment = 'scalar_pc_c3';
SELECT id FROM scalar_pc WHERE v > 30 ORDER BY id LIMIT 5;
SELECT id FROM scalar_pc WHERE v > 30 ORDER BY id LIMIT 5;

-- C4: ORDER BY a non-key column (Full sorting).
SET log_comment = 'scalar_pc_c4';
SELECT id FROM scalar_pc WHERE v > 40 ORDER BY v LIMIT 5;
SELECT id FROM scalar_pc WHERE v > 40 ORDER BY v LIMIT 5;

-- C5: JOIN with the literal in WHERE (not in ON).
SET log_comment = 'scalar_pc_c5';
SELECT count() FROM scalar_pc AS a INNER JOIN scalar_pc_join AS b ON a.id = b.id WHERE a.v > 50;
SELECT count() FROM scalar_pc AS a INNER JOIN scalar_pc_join AS b ON a.id = b.id WHERE a.v > 50;

-- C6: HAVING literal without a sibling identifier.
SET log_comment = 'scalar_pc_c6';
SELECT k, sum(v) FROM scalar_pc GROUP BY k HAVING sum(v) > 40000 ORDER BY k;
SELECT k, sum(v) FROM scalar_pc GROUP BY k HAVING sum(v) > 40000 ORDER BY k;

-- C7: window function.
SET log_comment = 'scalar_pc_c7';
SELECT id, row_number() OVER (PARTITION BY k ORDER BY id) AS rn FROM scalar_pc WHERE v > 900 ORDER BY id LIMIT 5;
SELECT id, row_number() OVER (PARTITION BY k ORDER BY id) AS rn FROM scalar_pc WHERE v > 900 ORDER BY id LIMIT 5;

-- C8: DISTINCT.
SET log_comment = 'scalar_pc_c8';
SELECT DISTINCT k FROM scalar_pc WHERE v > 60 ORDER BY k;
SELECT DISTINCT k FROM scalar_pc WHERE v > 60 ORDER BY k;

-- C9: Memory engine source (versioning is off, so it is cacheable in mode 0).
SET log_comment = 'scalar_pc_c9';
SELECT count() FROM scalar_pc_mem WHERE v > 70;
SELECT count() FROM scalar_pc_mem WHERE v > 70;

-- N1: no bindable literal (LIMIT values are not parameters).
SET log_comment = 'scalar_pc_n1';
SELECT id FROM scalar_pc ORDER BY id LIMIT 5;
SELECT id FROM scalar_pc ORDER BY id LIMIT 5;

-- N2: literal only in the JOIN ON clause.
SET log_comment = 'scalar_pc_n2';
SELECT count() FROM scalar_pc AS a INNER JOIN scalar_pc_join AS b ON a.id = b.id AND a.v > 80;
SELECT count() FROM scalar_pc AS a INNER JOIN scalar_pc_join AS b ON a.id = b.id AND a.v > 80;

-- N3: subquery.
SET log_comment = 'scalar_pc_n3';
SELECT count() FROM scalar_pc WHERE id IN (SELECT id FROM scalar_pc WHERE v > 900);
SELECT count() FROM scalar_pc WHERE id IN (SELECT id FROM scalar_pc WHERE v > 900);

-- N4: WITH clause.
SET log_comment = 'scalar_pc_n4';
WITH 4 AS x SELECT count() FROM scalar_pc WHERE v > 90 AND id > x;
WITH 4 AS x SELECT count() FROM scalar_pc WHERE v > 90 AND id > x;

-- N5: system table.
SET log_comment = 'scalar_pc_n5';
SELECT count() FROM system.tables WHERE name = 'scalar_pc';
SELECT count() FROM system.tables WHERE name = 'scalar_pc';

-- N6: unsupported function (`hex` makes the whole query non-cacheable).
SET log_comment = 'scalar_pc_n6';
SELECT count() FROM scalar_pc WHERE hex(k) = '1';
SELECT count() FROM scalar_pc WHERE hex(k) = '1';

-- N7: UNION ALL. `FORMAT Null` keeps the assertion about the cache counters only: the outer
-- ORDER BY of a UNION ALL belongs to the last branch, so the printed row order is not stable.
SET log_comment = 'scalar_pc_n7';
SELECT id FROM scalar_pc WHERE v > 990 UNION ALL SELECT id FROM scalar_pc WHERE v > 995 ORDER BY id FORMAT Null;
SELECT id FROM scalar_pc WHERE v > 990 UNION ALL SELECT id FROM scalar_pc WHERE v > 995 ORDER BY id FORMAT Null;

-- N8: tuple literal.
SET log_comment = 'scalar_pc_n8';
SELECT count() FROM scalar_pc WHERE (k, v) = (1, 1);
SELECT count() FROM scalar_pc WHERE (k, v) = (1, 1);

SET vector_query_plan_cache = 0;
SYSTEM FLUSH LOGS;

SELECT
    log_comment,
    sum(ProfileEvents['VectorQueryPlanCacheHits']) AS plan_hits,
    sum(ProfileEvents['VectorQueryPlanCacheMisses']) AS plan_misses,
    sum(ProfileEvents['VectorASTCacheHits']) AS ast_hits,
    sum(ProfileEvents['VectorASTCacheMisses']) AS ast_misses
FROM system.query_log
WHERE type = 'QueryFinish'
    AND (log_comment LIKE 'scalar_pc_c%' OR log_comment LIKE 'scalar_pc_n%')
    AND current_database = currentDatabase()
    AND event_time_microseconds > (SELECT started FROM scalar_pc_run_state)
GROUP BY log_comment
ORDER BY log_comment;

SELECT 'entries', value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries';

DROP TABLE IF EXISTS scalar_pc;
DROP TABLE IF EXISTS scalar_pc_join;
DROP TABLE IF EXISTS scalar_pc_mem;
DROP TABLE scalar_pc_run_state;

SYSTEM DROP VECTOR QUERY PLAN CACHE;
