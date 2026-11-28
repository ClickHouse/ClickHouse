-- Capping join key NDV by subtree rows must not flip the plan between a cold and a warm run:
-- with real column statistics the cap's inputs come from the catalog, so hash table statistics
-- collected during execution must not change the plan. Join lines are compared verbatim.

SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'auto';
SET use_hash_table_stats_for_join_reordering = 1;
SET collect_hash_table_stats_during_joins = 1;
SET join_algorithm = 'parallel_hash';
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS ndv_stab_f;
DROP TABLE IF EXISTS ndv_stab_d2;
DROP TABLE IF EXISTS ndv_stab_d1;
CREATE TABLE ndv_stab_f (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ndv_stab_d2 (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE ndv_stab_d1 (w UInt64, y UInt16) ENGINE = MergeTree ORDER BY w SETTINGS auto_statistics_types = 'uniq';
INSERT INTO ndv_stab_f SELECT number % 100, number FROM numbers(10000);
INSERT INTO ndv_stab_d2 SELECT number, intDiv(number, 10) FROM numbers(1000);
INSERT INTO ndv_stab_d1 SELECT number, 1999 FROM numbers(10);
ALTER TABLE ndv_stab_f MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 1;
ALTER TABLE ndv_stab_d2 MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 1;
ALTER TABLE ndv_stab_d1 MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 1;

-- Hash table statistics are process-wide and keyed by storage identity, not by the join order
-- algorithm. Give each algorithm its own tables so its first EXPLAIN is really cold.
CREATE TABLE ndv_stab_dphyp_f AS ndv_stab_f;
CREATE TABLE ndv_stab_dphyp_d2 AS ndv_stab_d2;
CREATE TABLE ndv_stab_dphyp_d1 AS ndv_stab_d1;
INSERT INTO ndv_stab_dphyp_f SELECT * FROM ndv_stab_f;
INSERT INTO ndv_stab_dphyp_d2 SELECT * FROM ndv_stab_d2;
INSERT INTO ndv_stab_dphyp_d1 SELECT * FROM ndv_stab_d1;

CREATE TABLE ndv_stab_dpsize_f AS ndv_stab_f;
CREATE TABLE ndv_stab_dpsize_d2 AS ndv_stab_d2;
CREATE TABLE ndv_stab_dpsize_d1 AS ndv_stab_d1;
INSERT INTO ndv_stab_dpsize_f SELECT * FROM ndv_stab_f;
INSERT INTO ndv_stab_dpsize_d2 SELECT * FROM ndv_stab_d2;
INSERT INTO ndv_stab_dpsize_d1 SELECT * FROM ndv_stab_d1;

CREATE TABLE ndv_stab_greedy_f AS ndv_stab_f;
CREATE TABLE ndv_stab_greedy_d2 AS ndv_stab_d2;
CREATE TABLE ndv_stab_greedy_d1 AS ndv_stab_d1;
INSERT INTO ndv_stab_greedy_f SELECT * FROM ndv_stab_f;
INSERT INTO ndv_stab_greedy_d2 SELECT * FROM ndv_stab_d2;
INSERT INTO ndv_stab_greedy_d1 SELECT * FROM ndv_stab_d1;

CREATE TEMPORARY TABLE cold_plan (explain String) ENGINE = Memory;
CREATE TEMPORARY TABLE hot_plan (explain String) ENGINE = Memory;

INSERT INTO cold_plan SELECT explain FROM (EXPLAIN
SELECT count() FROM ndv_stab_f AS f
INNER JOIN ndv_stab_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_stab_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub');

SELECT count() FROM ndv_stab_f AS f
INNER JOIN ndv_stab_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_stab_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub' FORMAT Null;

INSERT INTO hot_plan SELECT explain FROM (EXPLAIN
SELECT count() FROM ndv_stab_f AS f
INNER JOIN ndv_stab_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_stab_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub');

SELECT 'dpsub',
    (SELECT groupArray(explain) FROM cold_plan WHERE explain LIKE '%⋈%')
    = (SELECT groupArray(explain) FROM hot_plan WHERE explain LIKE '%⋈%'),
    (SELECT count() FROM hot_plan WHERE explain LIKE '%f[10000] ⋈ (d2[1000] ⋈ d1[10])%') = 1;

TRUNCATE TABLE cold_plan;
TRUNCATE TABLE hot_plan;
INSERT INTO cold_plan SELECT explain FROM (EXPLAIN
SELECT count() FROM ndv_stab_dphyp_f AS f
INNER JOIN ndv_stab_dphyp_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_stab_dphyp_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp');

SELECT count() FROM ndv_stab_dphyp_f AS f
INNER JOIN ndv_stab_dphyp_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_stab_dphyp_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp' FORMAT Null;

INSERT INTO hot_plan SELECT explain FROM (EXPLAIN
SELECT count() FROM ndv_stab_dphyp_f AS f
INNER JOIN ndv_stab_dphyp_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_stab_dphyp_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp');

SELECT 'dphyp',
    (SELECT groupArray(explain) FROM cold_plan WHERE explain LIKE '%⋈%')
    = (SELECT groupArray(explain) FROM hot_plan WHERE explain LIKE '%⋈%'),
    (SELECT count() FROM hot_plan WHERE explain LIKE '%f[10000] ⋈ (d2[1000] ⋈ d1[10])%') = 1;

TRUNCATE TABLE cold_plan;
TRUNCATE TABLE hot_plan;
INSERT INTO cold_plan SELECT explain FROM (EXPLAIN
SELECT count() FROM ndv_stab_dpsize_f AS f
INNER JOIN ndv_stab_dpsize_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_stab_dpsize_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize');

SELECT count() FROM ndv_stab_dpsize_f AS f
INNER JOIN ndv_stab_dpsize_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_stab_dpsize_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize' FORMAT Null;

INSERT INTO hot_plan SELECT explain FROM (EXPLAIN
SELECT count() FROM ndv_stab_dpsize_f AS f
INNER JOIN ndv_stab_dpsize_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_stab_dpsize_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize');

SELECT 'dpsize',
    (SELECT groupArray(explain) FROM cold_plan WHERE explain LIKE '%⋈%')
    = (SELECT groupArray(explain) FROM hot_plan WHERE explain LIKE '%⋈%'),
    (SELECT count() FROM hot_plan WHERE explain LIKE '%f[10000] ⋈ (d2[1000] ⋈ d1[10])%') = 1;

TRUNCATE TABLE cold_plan;
TRUNCATE TABLE hot_plan;
INSERT INTO cold_plan SELECT explain FROM (EXPLAIN
SELECT count() FROM ndv_stab_greedy_f AS f
INNER JOIN ndv_stab_greedy_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_stab_greedy_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy');

SELECT count() FROM ndv_stab_greedy_f AS f
INNER JOIN ndv_stab_greedy_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_stab_greedy_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy' FORMAT Null;

INSERT INTO hot_plan SELECT explain FROM (EXPLAIN
SELECT count() FROM ndv_stab_greedy_f AS f
INNER JOIN ndv_stab_greedy_d2 AS d2 ON f.k = d2.k
INNER JOIN ndv_stab_greedy_d1 AS d1 ON d2.w = d1.w
WHERE d1.y = 1999
SETTINGS query_plan_optimize_join_order_algorithm = 'greedy');

SELECT 'greedy',
    (SELECT groupArray(explain) FROM cold_plan WHERE explain LIKE '%⋈%')
    = (SELECT groupArray(explain) FROM hot_plan WHERE explain LIKE '%⋈%'),
    (SELECT count() FROM hot_plan WHERE explain LIKE '%f[10000] ⋈ (d2[1000] ⋈ d1[10])%') = 1;

DROP TABLE ndv_stab_f;
DROP TABLE ndv_stab_d2;
DROP TABLE ndv_stab_d1;
DROP TABLE ndv_stab_dphyp_f;
DROP TABLE ndv_stab_dphyp_d2;
DROP TABLE ndv_stab_dphyp_d1;
DROP TABLE ndv_stab_dpsize_f;
DROP TABLE ndv_stab_dpsize_d2;
DROP TABLE ndv_stab_dpsize_d1;
DROP TABLE ndv_stab_greedy_f;
DROP TABLE ndv_stab_greedy_d2;
DROP TABLE ndv_stab_greedy_d1;
DROP TABLE cold_plan;
DROP TABLE hot_plan;
