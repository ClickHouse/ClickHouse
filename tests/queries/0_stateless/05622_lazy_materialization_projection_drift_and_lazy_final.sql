-- Lazy materialization over a join, with a projection or lazy FINAL changing the read after the plan was split.
-- Each query is compared with the optimization off.

SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_lazy_materialization_for_join = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_drift;
DROP TABLE IF EXISTS t_final;
DROP TABLE IF EXISTS r;

CREATE TABLE r (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO r SELECT number, toString(number) FROM numbers(100);

SELECT '-- a projection part without a column read lazily';
-- The projection stores the column the alias names. After the alias changes, the old projection part does not have the
-- column the projection has now, so its rows, the lazily read column too, are read from the parent part.
CREATE TABLE t_drift (id UInt64, k UInt64, b String, d String, c String ALIAS b, PROJECTION p (SELECT id, k, c ORDER BY id))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_drift;
INSERT INTO t_drift SELECT number, number % 100, 'before', 'after' FROM numbers(10000);
ALTER TABLE t_drift MODIFY COLUMN c String ALIAS d;
-- The new part has the column in its projection part.
INSERT INTO t_drift SELECT number, number % 100, 'before', 'after' FROM numbers(10000, 10000);

-- The projection filters by `id`, so it serves the read.
SELECT t.id, t.c, r.v FROM t_drift AS t JOIN r ON t.k = r.k WHERE t.id IN (1, 10001) ORDER BY t.id LIMIT 2
SETTINGS optimize_move_to_prewhere = 0, optimize_use_projections = 1;
SELECT t.id, t.c, r.v FROM t_drift AS t JOIN r ON t.k = r.k WHERE t.id IN (1, 10001) ORDER BY t.id LIMIT 2
SETTINGS optimize_move_to_prewhere = 0, optimize_use_projections = 1, query_plan_optimize_lazy_materialization = 0;
SELECT count() > 0 FROM (
    EXPLAIN SELECT t.id, t.c, r.v FROM t_drift AS t JOIN r ON t.k = r.k WHERE t.id IN (1, 10001) ORDER BY t.id LIMIT 2
    SETTINGS optimize_move_to_prewhere = 0, optimize_use_projections = 1)
WHERE explain LIKE '%ReadFromMergeTree (p)%';

SELECT '-- lazy FINAL reads ranges of one part in both of its branches';
-- A merged part has all keys, and a newer part some of them in the middle: lazy FINAL reads the ranges of the merged
-- part that intersect the newer part with FINAL, and the others without.
CREATE TABLE t_final (k UInt64, v UInt64, status String, payload String)
ENGINE = ReplacingMergeTree(v) ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO t_final SELECT number, 1, if(number % 7 = 0, 'active', 'inactive'), concat('old', toString(number)) FROM numbers(10000);
OPTIMIZE TABLE t_final FINAL;
SYSTEM STOP MERGES t_final;
INSERT INTO t_final SELECT number, 2, if(number % 7 = 0, 'active', 'inactive'), concat('new', toString(number)) FROM numbers(4000, 2000);

SELECT t.k, t.payload, r.v FROM t_final AS t FINAL JOIN r ON t.k % 100 = r.k WHERE t.status = 'active' ORDER BY sipHash64(t.k) LIMIT 5
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0;
SELECT t.k, t.payload, r.v FROM t_final AS t FINAL JOIN r ON t.k % 100 = r.k WHERE t.status = 'active' ORDER BY sipHash64(t.k) LIMIT 5
SETTINGS query_plan_optimize_lazy_final = 0, query_plan_optimize_lazy_materialization = 0;
SELECT count() > 0 FROM (
    EXPLAIN SELECT t.k, t.payload, r.v FROM t_final AS t FINAL JOIN r ON t.k % 100 = r.k WHERE t.status = 'active' ORDER BY sipHash64(t.k) LIMIT 5
    SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0)
WHERE explain LIKE '%InputSelector%';

DROP TABLE t_drift;
DROP TABLE t_final;
DROP TABLE r;
