-- Tags: no-parallel-replicas
-- no-parallel-replicas: EXPLAIN ANALYZE rejects distributed plans (NOT_IMPLEMENTED).

-- `EXPLAIN ANALYZE` prints the q-error of each join side's estimated rows next to `rows estimated` and `rows`.
-- The side `d` is a filtered table read, so its estimate is the filter estimate from the `uniq` statistics:
-- 1000 rows, `v` has 10 distinct values (100 rows each), `s` has 2 ('rare' is 20 rows, 'common' 980, both estimated 500).

SET use_statistics = 1, materialize_statistics_on_insert = 1;
SET enable_join_runtime_filters = 0; -- a runtime filter would lower the actual rows of a side without any estimation error
SET query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 0;
SET use_hash_table_stats_for_join_reordering = 0, collect_hash_table_stats_during_joins = 0;
SET parallel_hash_join_threshold = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET query_plan_optimize_join_order_limit = 10;
SET max_insert_threads = 1;

DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS d;
CREATE TABLE f (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE d (k UInt64, v UInt64, s String) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'uniq';
INSERT INTO f SELECT number FROM numbers(1000);
INSERT INTO d SELECT number, number % 10, if(number % 50 = 0, 'rare', 'common') FROM numbers(1000);

-- Exact estimates on both sides.
SELECT
    countIf(explain LIKE '%: rows estimated 100.00 · rows 100.00 · q-error 1.00 · matched%') = 1,
    countIf(explain LIKE '%: rows estimated 1.00 thousand · rows 1.00 thousand · q-error 1.00 · matched%') = 1
FROM (EXPLAIN ANALYZE SELECT * FROM f JOIN d ON f.k = d.k WHERE d.v = 3);

-- A wrong filter estimate: 500 estimated, 20 actual.
SELECT
    countIf(explain LIKE '%: rows estimated 500.00 · rows 20.00 · q-error 25.00 · matched%') = 1,
    countIf(explain LIKE '%: rows estimated 1.00 thousand · rows 1.00 thousand · q-error 1.00 · matched%') = 1
FROM (EXPLAIN ANALYZE SELECT * FROM f JOIN d ON f.k = d.k WHERE d.s = 'rare');

-- An underestimated filter: 500 estimated, 980 actual.
SELECT
    countIf(explain LIKE '%: rows estimated 500.00 · rows 980.00 · q-error 1.96 · matched%') = 1,
    countIf(explain LIKE '%: rows estimated 1.00 thousand · rows 1.00 thousand · q-error 1.00 · matched%') = 1
FROM (EXPLAIN ANALYZE SELECT * FROM f JOIN d ON f.k = d.k WHERE d.s = 'common');

-- No q-error for a side with 0 actual rows.
SELECT
    countIf(explain LIKE '%: rows estimated 100.00 · rows 0.00 · matched%') = 1,
    countIf(explain LIKE '%rows 0.00 · q-error%') = 0
FROM (EXPLAIN ANALYZE SELECT * FROM f JOIN d ON f.k = d.k WHERE d.v = 42);

-- No q-error without estimates.
SELECT
    countIf(explain LIKE '%rows estimated no stats%') = 2,
    countIf(explain LIKE '%q-error%') = 0
FROM (EXPLAIN ANALYZE SELECT * FROM f JOIN d ON f.k = d.k WHERE d.v = 3
      SETTINGS query_plan_optimize_join_order_limit = 0);

DROP TABLE f;
DROP TABLE d;
