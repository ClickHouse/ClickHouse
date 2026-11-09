-- `merge` optimizes the plan of the view again: the second run must not tag the filter with the late copy either
SET enable_parallel_replicas = 0, enable_join_runtime_filters = 0, use_query_condition_cache = 1;
SET optimize_move_to_prewhere = 0, query_plan_propagate_predicate_across_join = 1;
SET query_plan_join_swap_table = 'false', query_plan_optimize_join_order_limit = 0;

DROP VIEW IF EXISTS late_copy_qcc_merge_v;
DROP TABLE IF EXISTS late_copy_qcc_merge_s;
DROP TABLE IF EXISTS late_copy_qcc_merge_d;

CREATE TABLE late_copy_qcc_merge_s (k UInt64) ENGINE = Memory;
CREATE TABLE late_copy_qcc_merge_d (k UInt64, pad UInt64) ENGINE = MergeTree ORDER BY pad SETTINGS index_granularity = 8192;
INSERT INTO late_copy_qcc_merge_s VALUES (42);
INSERT INTO late_copy_qcc_merge_d SELECT number, number FROM numbers(100000);

CREATE VIEW late_copy_qcc_merge_v AS
SELECT d.k AS k FROM (SELECT k FROM late_copy_qcc_merge_s WHERE k = 42) AS s
INNER JOIN (SELECT k FROM late_copy_qcc_merge_d WHERE pad % 2 = 0) AS d ON s.k = d.k;

SELECT 'merge', count() FROM merge(currentDatabase(), '^late_copy_qcc_merge_v$');

SELECT 'target filter alone', count() FROM late_copy_qcc_merge_d WHERE pad % 2 = 0;

DROP VIEW late_copy_qcc_merge_v;
DROP TABLE late_copy_qcc_merge_s;
DROP TABLE late_copy_qcc_merge_d;
