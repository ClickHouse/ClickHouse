-- A multi-relation subtree cannot contain more distinct join keys than rows.
-- The outer join must therefore use selectivity 1 / 100, not d2.k's base-table NDV.

SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'auto';
SET use_hash_table_stats_for_join_reordering = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS ndv_f;
DROP TABLE IF EXISTS ndv_f2;
DROP TABLE IF EXISTS ndv_d2;
DROP TABLE IF EXISTS ndv_d1;
CREATE TABLE ndv_f (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ndv_f2 (v UInt64) ENGINE = MergeTree ORDER BY v SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ndv_d2 (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ndv_d1 (w UInt64, y UInt16) ENGINE = MergeTree ORDER BY w SETTINGS auto_statistics_types = 'uniq';
INSERT INTO ndv_f SELECT number % 100, number FROM numbers(10000);
INSERT INTO ndv_f2 SELECT number FROM numbers(10000);
INSERT INTO ndv_d2 SELECT number, intDiv(number, 10) FROM numbers(1000);
INSERT INTO ndv_d1 SELECT number, 1999 FROM numbers(10);
ALTER TABLE ndv_f MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 1;
ALTER TABLE ndv_f2 MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 1;
ALTER TABLE ndv_d2 MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 1;
ALTER TABLE ndv_d1 MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 1;

SELECT 'dpsub direct',
    countIf(explain LIKE '%f[10000] ⋈ (d2[1000] ⋈ d1[10])%') = 1,
    countIf(explain LIKE '%Selectivity: estimated (NDV) 0.01%') = 2,
    countIf(explain LIKE '%Output rows: estimated 10.00 thousand · actual 10.00 thousand · q-error 1.00%') = 1,
    countIf(explain LIKE '%Output rows: estimated 100.00 · actual 100.00 · q-error 1.00%') = 1
FROM (EXPLAIN ANALYZE indexes = 0
SELECT count() FROM ndv_f AS f
INNER JOIN ndv_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', enable_join_transitive_predicates = 0);

SELECT 'dpsub equivalence',
    countIf(explain LIKE '%f[10000] ⋈ (d2[1000] ⋈ d1[10])%') = 1,
    countIf(explain LIKE '%Selectivity: estimated (NDV) 0.01%') = 2,
    countIf(explain LIKE '%Output rows: estimated 10.00 thousand · actual 10.00 thousand · q-error 1.00%') = 1,
    countIf(explain LIKE '%Output rows: estimated 100.00 · actual 100.00 · q-error 1.00%') = 1
FROM (EXPLAIN ANALYZE indexes = 0
SELECT count() FROM ndv_f AS f
INNER JOIN ndv_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', enable_join_transitive_predicates = 1);

SELECT 'dpsize direct',
    countIf(explain LIKE '%f[10000] ⋈ (d2[1000] ⋈ d1[10])%') = 1,
    countIf(explain LIKE '%Selectivity: estimated (NDV) 0.01%') = 2,
    countIf(explain LIKE '%Output rows: estimated 10.00 thousand · actual 10.00 thousand · q-error 1.00%') = 1,
    countIf(explain LIKE '%Output rows: estimated 100.00 · actual 100.00 · q-error 1.00%') = 1
FROM (EXPLAIN ANALYZE indexes = 0
SELECT count() FROM ndv_f AS f
INNER JOIN ndv_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_join_transitive_predicates = 0);

SELECT 'dpsize equivalence',
    countIf(explain LIKE '%f[10000] ⋈ (d2[1000] ⋈ d1[10])%') = 1,
    countIf(explain LIKE '%Selectivity: estimated (NDV) 0.01%') = 2,
    countIf(explain LIKE '%Output rows: estimated 10.00 thousand · actual 10.00 thousand · q-error 1.00%') = 1,
    countIf(explain LIKE '%Output rows: estimated 100.00 · actual 100.00 · q-error 1.00%') = 1
FROM (EXPLAIN ANALYZE indexes = 0
SELECT count() FROM ndv_f AS f
INNER JOIN ndv_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_join_transitive_predicates = 1);

SELECT 'dphyp direct',
    countIf(explain LIKE '%f[10000] ⋈ (d2[1000] ⋈ d1[10])%') = 1,
    countIf(explain LIKE '%Selectivity: estimated (NDV) 0.01%') = 2,
    countIf(explain LIKE '%Output rows: estimated 10.00 thousand · actual 10.00 thousand · q-error 1.00%') = 1,
    countIf(explain LIKE '%Output rows: estimated 100.00 · actual 100.00 · q-error 1.00%') = 1
FROM (EXPLAIN ANALYZE indexes = 0
SELECT count() FROM ndv_f AS f
INNER JOIN ndv_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_join_transitive_predicates = 0);

SELECT 'dphyp equivalence',
    countIf(explain LIKE '%f[10000] ⋈ (d2[1000] ⋈ d1[10])%') = 1,
    countIf(explain LIKE '%Selectivity: estimated (NDV) 0.01%') = 2,
    countIf(explain LIKE '%Output rows: estimated 10.00 thousand · actual 10.00 thousand · q-error 1.00%') = 1,
    countIf(explain LIKE '%Output rows: estimated 100.00 · actual 100.00 · q-error 1.00%') = 1
FROM (EXPLAIN ANALYZE indexes = 0
SELECT count() FROM ndv_f AS f
INNER JOIN ndv_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_join_transitive_predicates = 1);

SELECT 'greedy direct',
    countIf(explain LIKE '%f[10000] ⋈ (d2[1000] ⋈ d1[10])%') = 1,
    countIf(explain LIKE '%Selectivity: estimated (NDV) 0.01%') = 2,
    countIf(explain LIKE '%Output rows: estimated 10.00 thousand · actual 10.00 thousand · q-error 1.00%') = 1,
    countIf(explain LIKE '%Output rows: estimated 100.00 · actual 100.00 · q-error 1.00%') = 1
FROM (EXPLAIN ANALYZE indexes = 0
SELECT count() FROM ndv_f AS f
INNER JOIN ndv_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_join_transitive_predicates = 0);

SELECT 'greedy equivalence',
    countIf(explain LIKE '%f[10000] ⋈ (d2[1000] ⋈ d1[10])%') = 1,
    countIf(explain LIKE '%Selectivity: estimated (NDV) 0.01%') = 2,
    countIf(explain LIKE '%Output rows: estimated 10.00 thousand · actual 10.00 thousand · q-error 1.00%') = 1,
    countIf(explain LIKE '%Output rows: estimated 100.00 · actual 100.00 · q-error 1.00%') = 1
FROM (EXPLAIN ANALYZE indexes = 0
SELECT count() FROM ndv_f AS f
INNER JOIN ndv_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_join_transitive_predicates = 1);

-- Without f.k statistics, f's known row count is still an NDV upper bound. The cap must keep
-- the NDV used for costing consistent with the displayed upper bound: both joins return 100 rows.
CREATE TABLE ndv_partial_f (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO ndv_partial_f SELECT number FROM numbers(100);

SELECT 'greedy direct partial statistics',
    countIf(explain LIKE '%Selectivity: estimated (NDV) 0.01%') = 2,
    countIf(explain LIKE '%Output rows: estimated 100.00 · actual 100.00 · q-error 1.00%') = 2
FROM (EXPLAIN ANALYZE indexes = 0
SELECT count() FROM ndv_d2 AS d2
INNER JOIN ndv_d1 AS d1 ON d2.w = d1.w
INNER JOIN ndv_partial_f AS f ON d2.k = f.k
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_join_transitive_predicates = 0);

SELECT 'greedy equivalence partial statistics',
    countIf(explain LIKE '%Selectivity: estimated (NDV) 0.01%') = 2,
    countIf(explain LIKE '%Output rows: estimated 100.00 · actual 100.00 · q-error 1.00%') = 2
FROM (EXPLAIN ANALYZE indexes = 0
SELECT count() FROM ndv_d2 AS d2
INNER JOIN ndv_d1 AS d1 ON d2.w = d1.w
INNER JOIN ndv_partial_f AS f ON d2.k = f.k
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_join_transitive_predicates = 1);

DROP TABLE ndv_partial_f;

-- f.k = d2.k is costed both with d2 as a leaf and with d2 inside {d1, d2}.
-- A cached selectivity from the first context must not bypass the second context's row cap.
SELECT 'cache context',
    countIf(explain LIKE '%Selectivity: estimated (NDV) 0.01%') = 2,
    countIf(explain LIKE '%Output rows: estimated 100.00 · actual 100.00 · q-error 1.00%') = 1
FROM (EXPLAIN ANALYZE indexes = 0
SELECT count() FROM ndv_f AS f
INNER JOIN ndv_f2 AS f2 ON f.v = f2.v
INNER JOIN ndv_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', enable_join_transitive_predicates = 1);

DROP TABLE ndv_f;
DROP TABLE ndv_f2;
DROP TABLE ndv_d2;
DROP TABLE ndv_d1;

-- Both inputs of the top join are multi-relation subtrees, so both keys are capped: {a, b} has
-- 100 rows but `a.k` has 1000 distinct values, turning the selectivity from 1 / 1000 into 1 / 100.
DROP TABLE IF EXISTS ndv_a;
DROP TABLE IF EXISTS ndv_b;
DROP TABLE IF EXISTS ndv_c;
DROP TABLE IF EXISTS ndv_d;
CREATE TABLE ndv_a (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ndv_b (w UInt64) ENGINE = MergeTree ORDER BY w SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ndv_c (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ndv_d (w UInt64) ENGINE = MergeTree ORDER BY w SETTINGS auto_statistics_types = 'uniq';
INSERT INTO ndv_a SELECT number, number % 100 FROM numbers(1000);
INSERT INTO ndv_b SELECT number FROM numbers(10);
INSERT INTO ndv_c SELECT number % 100, number % 100 FROM numbers(1000);
INSERT INTO ndv_d SELECT number FROM numbers(10);
ALTER TABLE ndv_a MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 1;
ALTER TABLE ndv_b MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 1;
ALTER TABLE ndv_c MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 1;
ALTER TABLE ndv_d MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 1;

SELECT 'bushy both sides capped',
    countIf(explain LIKE '%c[1000] ⋈ d[10] ⋈ (a[1000] ⋈ b[10])%') = 1,
    countIf(explain LIKE '%Selectivity: estimated (NDV) 0.01 · actual (cartesian) 0.01%') = 3,
    countIf(explain LIKE '%Output rows: estimated 100.00 · actual 100.00 · q-error 1.00%') = 3
FROM (EXPLAIN ANALYZE indexes = 0
SELECT count() FROM ndv_a AS a
INNER JOIN ndv_b AS b ON a.w = b.w
INNER JOIN ndv_c AS c ON a.k = c.k
INNER JOIN ndv_d AS d ON c.w = d.w
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_join_transitive_predicates = 1);

DROP TABLE ndv_a;
DROP TABLE ndv_b;
DROP TABLE ndv_c;
DROP TABLE ndv_d;

-- The cap must not make a subtree the side with fewer distinct keys. {c, n} keeps 120 of the 1500
-- customers, fewer than the 1000 distinct `o.custkey` values, so it is a sample of the customer key
-- domain rather than a superset of `o.custkey`: the selectivity stays 1 / 1500, not 1 / 1000.
DROP TABLE IF EXISTS ndv_o;
DROP TABLE IF EXISTS ndv_c;
DROP TABLE IF EXISTS ndv_n;
CREATE TABLE ndv_o (custkey UInt64) ENGINE = MergeTree ORDER BY custkey SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ndv_c (custkey UInt64, nationkey UInt64) ENGINE = MergeTree ORDER BY custkey SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ndv_n (nationkey UInt64) ENGINE = MergeTree ORDER BY nationkey SETTINGS auto_statistics_types = 'uniq';
INSERT INTO ndv_o SELECT number % 1000 FROM numbers(15000);
INSERT INTO ndv_c SELECT number, number % 25 FROM numbers(1500);
INSERT INTO ndv_n SELECT number FROM numbers(2);
ALTER TABLE ndv_o MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 1;
ALTER TABLE ndv_c MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 1;
ALTER TABLE ndv_n MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 1;

SELECT 'dpsub direct sampled key domain',
    countIf(explain LIKE '%o[15000] ⋈ (c[1500] ⋈ n[2])%') = 1,
    countIf(explain LIKE '%Output rows: estimated 1.20 thousand · actual 1.20 thousand · q-error 1.00%') = 1
FROM (EXPLAIN ANALYZE indexes = 0
SELECT count() FROM ndv_o AS o
INNER JOIN ndv_c AS c ON o.custkey = c.custkey
INNER JOIN ndv_n AS n ON c.nationkey = n.nationkey
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', enable_join_transitive_predicates = 0);

SELECT 'dpsub equivalence sampled key domain',
    countIf(explain LIKE '%o[15000] ⋈ (c[1500] ⋈ n[2])%') = 1,
    countIf(explain LIKE '%Output rows: estimated 1.20 thousand · actual 1.20 thousand · q-error 1.00%') = 1
FROM (EXPLAIN ANALYZE indexes = 0
SELECT count() FROM ndv_o AS o
INNER JOIN ndv_c AS c ON o.custkey = c.custkey
INNER JOIN ndv_n AS n ON c.nationkey = n.nationkey
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', enable_join_transitive_predicates = 1);

SELECT 'greedy direct sampled key domain',
    countIf(explain LIKE '%o[15000] ⋈ (c[1500] ⋈ n[2])%') = 1,
    countIf(explain LIKE '%Output rows: estimated 1.20 thousand · actual 1.20 thousand · q-error 1.00%') = 1
FROM (EXPLAIN ANALYZE indexes = 0
SELECT count() FROM ndv_o AS o
INNER JOIN ndv_c AS c ON o.custkey = c.custkey
INNER JOIN ndv_n AS n ON c.nationkey = n.nationkey
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_join_transitive_predicates = 0);

SELECT 'greedy equivalence sampled key domain',
    countIf(explain LIKE '%o[15000] ⋈ (c[1500] ⋈ n[2])%') = 1,
    countIf(explain LIKE '%Output rows: estimated 1.20 thousand · actual 1.20 thousand · q-error 1.00%') = 1
FROM (EXPLAIN ANALYZE indexes = 0
SELECT count() FROM ndv_o AS o
INNER JOIN ndv_c AS c ON o.custkey = c.custkey
INNER JOIN ndv_n AS n ON c.nationkey = n.nationkey
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_join_transitive_predicates = 1);

DROP TABLE ndv_o;
DROP TABLE ndv_c;
DROP TABLE ndv_n;
