-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test asserts on the estimate labels of the logical join step.

-- Join reordering estimates a MergeTree read from the row count of its parts, which still includes the rows deleted by
-- lightweight deletes. The estimate must exclude them, so the relation whose rows are deleted goes to the build side.

SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_randomize = 0; -- Pinned because the test asserts on the estimate labels
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_join_swap_table = 'auto';
SET use_hash_table_stats_for_join_reordering = 0;
SET collect_hash_table_stats_during_joins = 0;
SET enable_join_runtime_filters = 0;
SET join_algorithm = 'hash';
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS t_lwd_join_e;
DROP TABLE IF EXISTS t_lwd_join_a;
DROP TABLE IF EXISTS t_lwd_join_e_no_stats;
DROP TABLE IF EXISTS t_lwd_join_a_no_stats;

CREATE TABLE t_lwd_join_e (k Int64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t_lwd_join_a (k Int64, payload String) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t_lwd_join_e_no_stats (k Int64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
CREATE TABLE t_lwd_join_a_no_stats (k Int64, payload String) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';

INSERT INTO t_lwd_join_e SELECT number FROM numbers(4000);
INSERT INTO t_lwd_join_a SELECT number, repeat('x', 16) FROM numbers(2000);
INSERT INTO t_lwd_join_e_no_stats SELECT number FROM numbers(4000);
INSERT INTO t_lwd_join_a_no_stats SELECT number, repeat('x', 16) FROM numbers(2000);

SELECT '-- before deletes --';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1
    SELECT count() FROM t_lwd_join_a AS a INNER JOIN t_lwd_join_e AS e ON a.k = e.k) WHERE explain LIKE '%Join:%';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1
    SELECT count() FROM t_lwd_join_a_no_stats AS a INNER JOIN t_lwd_join_e_no_stats AS e ON a.k = e.k) WHERE explain LIKE '%Join:%';

DELETE FROM t_lwd_join_e WHERE k < 3000 SETTINGS lightweight_deletes_sync = 2, lightweight_delete_mode = 'alter_update';
DELETE FROM t_lwd_join_e_no_stats WHERE k < 3000 SETTINGS lightweight_deletes_sync = 2, lightweight_delete_mode = 'alter_update';

SELECT '-- after deleting three quarters of the rows --';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1
    SELECT count() FROM t_lwd_join_a AS a INNER JOIN t_lwd_join_e AS e ON a.k = e.k) WHERE explain LIKE '%Join:%';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1
    SELECT count() FROM t_lwd_join_a_no_stats AS a INNER JOIN t_lwd_join_e_no_stats AS e ON a.k = e.k) WHERE explain LIKE '%Join:%';

DELETE FROM t_lwd_join_e WHERE k >= 0 SETTINGS lightweight_deletes_sync = 2, lightweight_delete_mode = 'alter_update';
DELETE FROM t_lwd_join_e_no_stats WHERE k >= 0 SETTINGS lightweight_deletes_sync = 2, lightweight_delete_mode = 'alter_update';

SELECT '-- after deleting every row --';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1
    SELECT count() FROM t_lwd_join_a AS a INNER JOIN t_lwd_join_e AS e ON a.k = e.k) WHERE explain LIKE '%Join:%';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1
    SELECT count() FROM t_lwd_join_a_no_stats AS a INNER JOIN t_lwd_join_e_no_stats AS e ON a.k = e.k) WHERE explain LIKE '%Join:%';

SELECT count() FROM t_lwd_join_a AS a INNER JOIN t_lwd_join_e AS e ON a.k = e.k;
SELECT count() FROM t_lwd_join_a_no_stats AS a INNER JOIN t_lwd_join_e_no_stats AS e ON a.k = e.k;

DROP TABLE t_lwd_join_e;
DROP TABLE t_lwd_join_a;
DROP TABLE t_lwd_join_e_no_stats;
DROP TABLE t_lwd_join_a_no_stats;
