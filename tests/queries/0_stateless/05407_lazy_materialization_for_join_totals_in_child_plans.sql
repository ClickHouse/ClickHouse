-- Lazy materialization over joins is not used where a source can produce a block of totals, also when the source keeps
-- the plan that produces it as a plan of its own: a view with `SQL SECURITY DEFINER`, which is read as a sealed plan,
-- and a `Merge` table over it. The results are compared with the optimization disabled.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
-- Pinned so that the joins keep the order they are written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS merged;
DROP VIEW IF EXISTS totals_view;
DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE VIEW totals_view SQL SECURITY DEFINER AS SELECT k, count() AS c FROM r GROUP BY k WITH TOTALS;
CREATE TABLE merged (k UInt64, c UInt64) ENGINE = Merge(currentDatabase(), '^totals_view$');

INSERT INTO l SELECT number, concat('p', toString(number)) FROM numbers(10);
INSERT INTO r SELECT number % 5, number FROM numbers(20);

SELECT '-- the view on the right side';
SELECT l.k, l.payload, x.c FROM l JOIN totals_view AS x ON l.k = x.k ORDER BY l.k LIMIT 2 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.payload, x.c FROM l JOIN totals_view AS x ON l.k = x.k ORDER BY l.k LIMIT 2 SETTINGS query_plan_lazy_materialization_for_join = 0;

SELECT '-- the view on the left side';
SELECT x.k, x.c, l.payload FROM totals_view AS x LEFT JOIN l ON x.k = l.k ORDER BY x.c, x.k LIMIT 2
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT x.k, x.c, l.payload FROM totals_view AS x LEFT JOIN l ON x.k = l.k ORDER BY x.c, x.k LIMIT 2
SETTINGS query_plan_lazy_materialization_for_join = 0;

SELECT '-- a Merge table over the view';
SELECT l.k, l.payload, x.c FROM l JOIN merged AS x ON l.k = x.k ORDER BY l.k LIMIT 2 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.payload, x.c FROM l JOIN merged AS x ON l.k = x.k ORDER BY l.k LIMIT 2 SETTINGS query_plan_lazy_materialization_for_join = 0;

SELECT '-- nothing is read lazily';
SELECT countIf(explain LIKE '%LazilyRead%') FROM (EXPLAIN SELECT l.k, l.payload, x.c FROM l JOIN totals_view AS x ON l.k = x.k ORDER BY l.k LIMIT 2);

DROP TABLE merged;
DROP VIEW totals_view;
DROP TABLE l;
DROP TABLE r;
