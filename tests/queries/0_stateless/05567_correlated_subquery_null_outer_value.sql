-- An outer row whose correlated column is NULL must get the result of the correlated subquery evaluated for NULL
-- (https://github.com/ClickHouse/ClickHouse/issues/112576).

DROP TABLE IF EXISTS d_t;
DROP TABLE IF EXISTS n_t;
DROP TABLE IF EXISTS m_t;

CREATE TABLE d_t (c2 Nullable(Int32)) ENGINE = Memory;
CREATE TABLE n_t (x Nullable(Int32)) ENGINE = Memory;
CREATE TABLE m_t (a Nullable(Int32), b Int32) ENGINE = Memory;
INSERT INTO d_t VALUES (1), (NULL);
INSERT INTO n_t VALUES (7);
INSERT INTO m_t VALUES (NULL, 1), (NULL, 0), (1, 1);

-- { echo }

-- Scalar subqueries
SELECT d.c2, (SELECT count() FROM n_t n WHERE d.c2 IS NOT NULL OR d.c2 IS NULL) FROM d_t d ORDER BY d.c2 NULLS LAST;
SELECT d.c2, (SELECT sum(n.x) FROM n_t n WHERE d.c2 IS NOT NULL OR d.c2 IS NULL), (SELECT groupArray(n.x) FROM n_t n WHERE d.c2 IS NOT NULL OR d.c2 IS NULL), (SELECT any(n.x) FROM n_t n WHERE d.c2 IS NOT NULL OR d.c2 IS NULL) FROM d_t d ORDER BY d.c2 NULLS LAST;
SELECT d.c2, (SELECT groupArray(n.x) FROM n_t n WHERE d.c2 IS NULL) FROM d_t d ORDER BY d.c2 NULLS LAST;
SELECT d.c2, (SELECT n.x FROM n_t n WHERE d.c2 IS NULL) FROM d_t d ORDER BY d.c2 NULLS LAST;

-- EXISTS in the projection
SELECT r.v, EXISTS (SELECT 1 FROM values('a Nullable(Int32)', (NULL), (1)) t WHERE isNotDistinctFrom(t.a, r.v)) FROM values('v Nullable(Int32)', (NULL), (1), (2)) r ORDER BY r.v NULLS LAST;
SELECT r.v, EXISTS (SELECT 1 FROM values('a Nullable(Int32)', (NULL), (1)) t WHERE r.v IS NULL) FROM values('v Nullable(Int32)', (NULL), (1), (2)) r ORDER BY r.v NULLS LAST;

-- [NOT] EXISTS in WHERE
SELECT d.c2 FROM d_t d WHERE EXISTS (SELECT 1 FROM n_t n WHERE d.c2 IS NULL) ORDER BY d.c2 NULLS LAST;
SELECT d.c2 FROM d_t d WHERE NOT EXISTS (SELECT 1 FROM n_t n WHERE d.c2 IS NULL) ORDER BY d.c2 NULLS LAST;

-- `NULL = NULL` is not true, so the NULL row has no match here
SELECT r.v, EXISTS (SELECT 1 FROM values('a Nullable(Int32)', (NULL), (1)) t WHERE t.a = r.v) FROM values('v Nullable(Int32)', (NULL), (1), (2)) r ORDER BY r.v NULLS LAST SETTINGS correlated_subqueries_substitute_equivalent_expressions = 1;
SELECT r.v, EXISTS (SELECT 1 FROM values('a Nullable(Int32)', (NULL), (1)) t WHERE t.a = r.v) FROM values('v Nullable(Int32)', (NULL), (1), (2)) r ORDER BY r.v NULLS LAST SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0;

-- Type wrappers and several correlated columns
SELECT r.v, (SELECT groupArray(n.x) FROM n_t n WHERE r.v IS NULL) FROM values('v LowCardinality(Nullable(String))', ('a'), (NULL)) r ORDER BY r.v NULLS LAST;
SELECT r.v, (SELECT groupArray(n.x) FROM n_t n WHERE r.v IS NULL) FROM (SELECT NULL AS v) r;
SELECT m.a, m.b, (SELECT groupArray(n.x) FROM n_t n WHERE m.a IS NULL AND m.b > 0) FROM m_t m ORDER BY m.a NULLS LAST, m.b;

-- Join layouts and algorithms
SELECT d.c2, (SELECT groupArray(n.x) FROM n_t n WHERE d.c2 IS NULL) FROM d_t d ORDER BY d.c2 NULLS LAST SETTINGS correlated_subqueries_default_join_kind = 'left', correlated_subqueries_use_in_memory_buffer = 0;
SELECT d.c2, (SELECT groupArray(n.x) FROM n_t n WHERE d.c2 IS NULL) FROM d_t d ORDER BY d.c2 NULLS LAST SETTINGS correlated_subqueries_default_join_kind = 'left', correlated_subqueries_use_in_memory_buffer = 0, join_algorithm = 'partial_merge';
SELECT d.c2, (SELECT groupArray(n.x) FROM n_t n WHERE d.c2 IS NULL) FROM d_t d ORDER BY d.c2 NULLS LAST SETTINGS correlated_subqueries_default_join_kind = 'left', correlated_subqueries_use_in_memory_buffer = 0, join_algorithm = 'full_sorting_merge';

-- The substituted equality keeps a plain key; the evaluation over the outer values compares null-safely
SELECT countIf(explain ILIKE '%Join conditions:%IS NULL%') FROM (EXPLAIN SELECT r.v, EXISTS (SELECT 1 FROM values('a Nullable(Int32)', (NULL), (1)) t WHERE t.a = r.v) FROM values('v Nullable(Int32)', (NULL), (1), (2)) r SETTINGS correlated_subqueries_substitute_equivalent_expressions = 1, explain_query_plan_default = 'pretty', query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0, join_algorithm = 'hash', correlated_subqueries_default_join_kind = 'right', correlated_subqueries_use_in_memory_buffer = 1);
SELECT countIf(explain ILIKE '%Join conditions:%IS NULL%') > 0 FROM (EXPLAIN SELECT r.v, EXISTS (SELECT 1 FROM values('a Nullable(Int32)', (NULL), (1)) t WHERE t.a = r.v) FROM values('v Nullable(Int32)', (NULL), (1), (2)) r SETTINGS correlated_subqueries_substitute_equivalent_expressions = 0, explain_query_plan_default = 'pretty', query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0, join_algorithm = 'hash', correlated_subqueries_default_join_kind = 'right', correlated_subqueries_use_in_memory_buffer = 1);

-- { echoOff }

DROP TABLE d_t;
DROP TABLE n_t;
DROP TABLE m_t;
