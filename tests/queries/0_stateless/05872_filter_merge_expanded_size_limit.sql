-- A filter is not merged with the expressions below it when its condition would then exceed max_expanded_ast_elements.
-- The condition of this nested `a + a` chain expands to 2^(depth + 2) + 1 nodes: 1048577 at depth 18, 16385 at depth 12.
-- Also checked: index and projection analysis skip such a condition.

EXPLAIN actions = 0, compact = 0, pretty = 0, description = 0
SELECT count() FROM (SELECT a17 + a17 AS a18 FROM (SELECT a16 + a16 AS a17 FROM (SELECT a15 + a15 AS a16 FROM (SELECT a14 + a14 AS a15 FROM (SELECT a13 + a13 AS a14 FROM (SELECT a12 + a12 AS a13 FROM (SELECT a11 + a11 AS a12 FROM (SELECT a10 + a10 AS a11 FROM (SELECT a9 + a9 AS a10 FROM (SELECT a8 + a8 AS a9 FROM (SELECT a7 + a7 AS a8 FROM (SELECT a6 + a6 AS a7 FROM (SELECT a5 + a5 AS a6 FROM (SELECT a4 + a4 AS a5 FROM (SELECT a3 + a3 AS a4 FROM (SELECT a2 + a2 AS a3 FROM (SELECT a1 + a1 AS a2 FROM (SELECT val + val AS a1 FROM (SELECT number - 1 AS val FROM numbers(1, 1)))))))))))))))))))) WHERE a18 <= 0;

SELECT count() FROM (SELECT a17 + a17 AS a18 FROM (SELECT a16 + a16 AS a17 FROM (SELECT a15 + a15 AS a16 FROM (SELECT a14 + a14 AS a15 FROM (SELECT a13 + a13 AS a14 FROM (SELECT a12 + a12 AS a13 FROM (SELECT a11 + a11 AS a12 FROM (SELECT a10 + a10 AS a11 FROM (SELECT a9 + a9 AS a10 FROM (SELECT a8 + a8 AS a9 FROM (SELECT a7 + a7 AS a8 FROM (SELECT a6 + a6 AS a7 FROM (SELECT a5 + a5 AS a6 FROM (SELECT a4 + a4 AS a5 FROM (SELECT a3 + a3 AS a4 FROM (SELECT a2 + a2 AS a3 FROM (SELECT a1 + a1 AS a2 FROM (SELECT val + val AS a1 FROM (SELECT number - 1 AS val FROM numbers(1, 1)))))))))))))))))))) WHERE a18 <= 0;

EXPLAIN actions = 0, compact = 0, pretty = 0, description = 0
SELECT count() FROM (SELECT a11 + a11 AS a12 FROM (SELECT a10 + a10 AS a11 FROM (SELECT a9 + a9 AS a10 FROM (SELECT a8 + a8 AS a9 FROM (SELECT a7 + a7 AS a8 FROM (SELECT a6 + a6 AS a7 FROM (SELECT a5 + a5 AS a6 FROM (SELECT a4 + a4 AS a5 FROM (SELECT a3 + a3 AS a4 FROM (SELECT a2 + a2 AS a3 FROM (SELECT a1 + a1 AS a2 FROM (SELECT val + val AS a1 FROM (SELECT number - 1 AS val FROM numbers(1, 1)))))))))))))) WHERE a12 <= 0;

EXPLAIN actions = 0, compact = 0, pretty = 0, description = 0
SELECT count() FROM (SELECT a11 + a11 AS a12 FROM (SELECT a10 + a10 AS a11 FROM (SELECT a9 + a9 AS a10 FROM (SELECT a8 + a8 AS a9 FROM (SELECT a7 + a7 AS a8 FROM (SELECT a6 + a6 AS a7 FROM (SELECT a5 + a5 AS a6 FROM (SELECT a4 + a4 AS a5 FROM (SELECT a3 + a3 AS a4 FROM (SELECT a2 + a2 AS a3 FROM (SELECT a1 + a1 AS a2 FROM (SELECT val + val AS a1 FROM (SELECT number - 1 AS val FROM numbers(1, 1)))))))))))))) WHERE a12 <= 0 SETTINGS max_expanded_ast_elements = 1000;

EXPLAIN actions = 0, compact = 0, pretty = 0, description = 0
SELECT count() FROM (SELECT val, a17 + a17 AS a18 FROM (SELECT val, a16 + a16 AS a17 FROM (SELECT val, a15 + a15 AS a16 FROM (SELECT val, a14 + a14 AS a15 FROM (SELECT val, a13 + a13 AS a14 FROM (SELECT val, a12 + a12 AS a13 FROM (SELECT val, a11 + a11 AS a12 FROM (SELECT val, a10 + a10 AS a11 FROM (SELECT val, a9 + a9 AS a10 FROM (SELECT val, a8 + a8 AS a9 FROM (SELECT val, a7 + a7 AS a8 FROM (SELECT val, a6 + a6 AS a7 FROM (SELECT val, a5 + a5 AS a6 FROM (SELECT val, a4 + a4 AS a5 FROM (SELECT val, a3 + a3 AS a4 FROM (SELECT val, a2 + a2 AS a3 FROM (SELECT val, a1 + a1 AS a2 FROM (SELECT val, val + val AS a1 FROM (SELECT number - 1 AS val FROM numbers(1, 1))))))))))))))))))) WHERE val >= 0) WHERE a18 <= 0 SETTINGS query_plan_split_filter = 0, query_plan_merge_filters = 1;

DROP TABLE IF EXISTS t_05872;
CREATE TABLE t_05872 (val Int64, PROJECTION p (SELECT val, count() GROUP BY val)) ENGINE = MergeTree ORDER BY val;
INSERT INTO t_05872 VALUES (0);

SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM (SELECT val, a17 + a17 AS a18 FROM (SELECT val, a16 + a16 AS a17 FROM (SELECT val, a15 + a15 AS a16 FROM (SELECT val, a14 + a14 AS a15 FROM (SELECT val, a13 + a13 AS a14 FROM (SELECT val, a12 + a12 AS a13 FROM (SELECT val, a11 + a11 AS a12 FROM (SELECT val, a10 + a10 AS a11 FROM (SELECT val, a9 + a9 AS a10 FROM (SELECT val, a8 + a8 AS a9 FROM (SELECT val, a7 + a7 AS a8 FROM (SELECT val, a6 + a6 AS a7 FROM (SELECT val, a5 + a5 AS a6 FROM (SELECT val, a4 + a4 AS a5 FROM (SELECT val, a3 + a3 AS a4 FROM (SELECT val, a2 + a2 AS a3 FROM (SELECT val, a1 + a1 AS a2 FROM (SELECT val, val + val AS a1 FROM (SELECT val FROM t_05872))))))))))))))))))) WHERE a18 <= 0 AND val = 0 SETTINGS optimize_use_projections = 0, enable_parallel_replicas = 0) WHERE explain ILIKE '%Condition%';

SELECT replace(trimLeft(explain), currentDatabase(), 'default') FROM (EXPLAIN SELECT count() FROM (SELECT val, a17 + a17 AS a18 FROM (SELECT val, a16 + a16 AS a17 FROM (SELECT val, a15 + a15 AS a16 FROM (SELECT val, a14 + a14 AS a15 FROM (SELECT val, a13 + a13 AS a14 FROM (SELECT val, a12 + a12 AS a13 FROM (SELECT val, a11 + a11 AS a12 FROM (SELECT val, a10 + a10 AS a11 FROM (SELECT val, a9 + a9 AS a10 FROM (SELECT val, a8 + a8 AS a9 FROM (SELECT val, a7 + a7 AS a8 FROM (SELECT val, a6 + a6 AS a7 FROM (SELECT val, a5 + a5 AS a6 FROM (SELECT val, a4 + a4 AS a5 FROM (SELECT val, a3 + a3 AS a4 FROM (SELECT val, a2 + a2 AS a3 FROM (SELECT val, a1 + a1 AS a2 FROM (SELECT val, val + val AS a1 FROM (SELECT val FROM t_05872))))))))))))))))))) WHERE a18 <= 0 SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 0, enable_parallel_replicas = 0) WHERE explain LIKE '%ReadFrom%';

DROP TABLE t_05872;

-- The walk of index analysis ends at such a condition: a LIMIT above it does not reach the source,
SELECT a12 FROM (SELECT a11 + a11 AS a12 FROM (SELECT a10 + a10 AS a11 FROM (SELECT a9 + a9 AS a10 FROM (SELECT a8 + a8 AS a9 FROM (SELECT a7 + a7 AS a8 FROM (SELECT a6 + a6 AS a7 FROM (SELECT a5 + a5 AS a6 FROM (SELECT a4 + a4 AS a5 FROM (SELECT a3 + a3 AS a4 FROM (SELECT a2 + a2 AS a3 FROM (SELECT a1 + a1 AS a2 FROM (SELECT val + val AS a1 FROM (SELECT number AS val FROM numbers(10)))))))))))))) WHERE a12 > 0 LIMIT 1 OFFSET 1 SETTINGS max_expanded_ast_elements = 1000;

-- and the query condition cache does not store its result under the key of another filter.
DROP TABLE IF EXISTS t_05872_memory;
CREATE TABLE t_05872_memory (k UInt64) ENGINE = Memory;
INSERT INTO t_05872_memory VALUES (0);
SELECT k FROM (SELECT k, a12 FROM (SELECT k, a11 + a11 AS a12 FROM (SELECT k, a10 + a10 AS a11 FROM (SELECT k, a9 + a9 AS a10 FROM (SELECT k, a8 + a8 AS a9 FROM (SELECT k, a7 + a7 AS a8 FROM (SELECT k, a6 + a6 AS a7 FROM (SELECT k, a5 + a5 AS a6 FROM (SELECT k, a4 + a4 AS a5 FROM (SELECT k, a3 + a3 AS a4 FROM (SELECT k, a2 + a2 AS a3 FROM (SELECT k, a1 + a1 AS a2 FROM (SELECT k, val + val AS a1 FROM (SELECT k, k AS val FROM t_05872_memory))))))))))))) WHERE a12 > 0) WHERE k = 0 SETTINGS max_expanded_ast_elements = 1000, query_plan_merge_filters = 0, query_plan_filter_push_down = 0, optimize_move_to_prewhere = 0, use_query_condition_cache = 1;
SELECT k FROM (SELECT k, a12 FROM (SELECT k, a11 + a11 AS a12 FROM (SELECT k, a10 + a10 AS a11 FROM (SELECT k, a9 + a9 AS a10 FROM (SELECT k, a8 + a8 AS a9 FROM (SELECT k, a7 + a7 AS a8 FROM (SELECT k, a6 + a6 AS a7 FROM (SELECT k, a5 + a5 AS a6 FROM (SELECT k, a4 + a4 AS a5 FROM (SELECT k, a3 + a3 AS a4 FROM (SELECT k, a2 + a2 AS a3 FROM (SELECT k, a1 + a1 AS a2 FROM (SELECT k, val + val AS a1 FROM (SELECT k, k AS val FROM t_05872_memory))))))))))))) WHERE a12 >= 0) WHERE k = 0 SETTINGS max_expanded_ast_elements = 1000, query_plan_merge_filters = 0, query_plan_filter_push_down = 0, optimize_move_to_prewhere = 0, use_query_condition_cache = 1;
DROP TABLE t_05872_memory;
