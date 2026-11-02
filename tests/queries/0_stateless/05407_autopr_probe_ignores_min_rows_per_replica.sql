-- The automatic mode ignores `parallel_replicas_min_number_of_rows_per_replica`. Building its plan with
-- parallel replicas used to run the row estimate this setting asks for, a full extra index analysis of the read.

SET enable_analyzer=1;

-- Mode 2 builds the plan with parallel replicas on every run, while mode 1 would decide each query after the first
-- from the statistics the first one collected, without building it.
SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';
SET query_plan_optimize_join_order_randomize=0;
-- Don't let the min-bytes gate skip the small reads before the plan with parallel replicas is built.
SET automatic_parallel_replicas_min_bytes_per_replica=0;
SET use_query_condition_cache=0;

CREATE TABLE sales (d Date, item UInt32, order_id UInt64) ENGINE = MergeTree ORDER BY (d, item);
CREATE TABLE returns (d Date, item UInt32, order_id UInt64) ENGINE = MergeTree ORDER BY (d, item);
INSERT INTO sales SELECT toDate('2026-01-01') + number % 365, number % 20000, number FROM numbers(1000000);
INSERT INTO returns SELECT toDate('2026-01-01') + number % 365, number % 20000, number * 10 FROM numbers(500000);

SELECT s.item, r.order_id FROM sales s, returns r
WHERE s.item = r.item AND s.order_id = r.order_id AND s.item BETWEEN 100 AND 120 AND r.item BETWEEN 100 AND 120
LIMIT 10 FORMAT Null SETTINGS log_comment = '05407_without_min_rows';

-- The setting given in the `SETTINGS` clause, which the nested build re-applies.
SELECT s.item, r.order_id FROM sales s, returns r
WHERE s.item = r.item AND s.order_id = r.order_id AND s.item BETWEEN 100 AND 120 AND r.item BETWEEN 100 AND 120
LIMIT 10 FORMAT Null SETTINGS log_comment = '05407_min_rows_in_query', parallel_replicas_min_number_of_rows_per_replica = 50000000;

SET parallel_replicas_min_number_of_rows_per_replica = 50000000;
SELECT s.item, r.order_id FROM sales s, returns r
WHERE s.item = r.item AND s.order_id = r.order_id AND s.item BETWEEN 100 AND 120 AND r.item BETWEEN 100 AND 120
LIMIT 10 FORMAT Null SETTINGS log_comment = '05407_min_rows_in_session';

-- The setting given in the `SETTINGS` clause of a subquery, of a view analyzed by its own interpreter (not inlined),
-- and of the `view` table function.
SET parallel_replicas_min_number_of_rows_per_replica = 0, parallel_replicas_plan_based = 1;
SET analyzer_inline_views = 0;

CREATE VIEW sales_view_without_min_rows AS SELECT * FROM sales SETTINGS parallel_replicas_min_number_of_rows_per_replica = 0;
CREATE VIEW sales_view AS SELECT * FROM sales SETTINGS parallel_replicas_min_number_of_rows_per_replica = 50000000;

SELECT s.item, r.order_id FROM (SELECT * FROM sales SETTINGS parallel_replicas_min_number_of_rows_per_replica = 0) s, returns r
WHERE s.item = r.item AND s.order_id = r.order_id AND s.item BETWEEN 100 AND 120 AND r.item BETWEEN 100 AND 120
LIMIT 10 FORMAT Null SETTINGS log_comment = '05407_subquery_without_min_rows';

SELECT s.item, r.order_id FROM (SELECT * FROM sales SETTINGS parallel_replicas_min_number_of_rows_per_replica = 50000000) s, returns r
WHERE s.item = r.item AND s.order_id = r.order_id AND s.item BETWEEN 100 AND 120 AND r.item BETWEEN 100 AND 120
LIMIT 10 FORMAT Null SETTINGS log_comment = '05407_min_rows_in_subquery';

SELECT s.item, r.order_id FROM sales_view_without_min_rows s, returns r
WHERE s.item = r.item AND s.order_id = r.order_id AND s.item BETWEEN 100 AND 120 AND r.item BETWEEN 100 AND 120
LIMIT 10 FORMAT Null SETTINGS log_comment = '05407_view_without_min_rows';

SELECT s.item, r.order_id FROM sales_view s, returns r
WHERE s.item = r.item AND s.order_id = r.order_id AND s.item BETWEEN 100 AND 120 AND r.item BETWEEN 100 AND 120
LIMIT 10 FORMAT Null SETTINGS log_comment = '05407_min_rows_in_view';

SELECT s.item, r.order_id FROM view(SELECT * FROM sales SETTINGS parallel_replicas_min_number_of_rows_per_replica = 0) s, returns r
WHERE s.item = r.item AND s.order_id = r.order_id AND s.item BETWEEN 100 AND 120 AND r.item BETWEEN 100 AND 120
LIMIT 10 FORMAT Null SETTINGS log_comment = '05407_view_function_without_min_rows';

SELECT s.item, r.order_id FROM view(SELECT * FROM sales SETTINGS parallel_replicas_min_number_of_rows_per_replica = 50000000) s, returns r
WHERE s.item = r.item AND s.order_id = r.order_id AND s.item BETWEEN 100 AND 120 AND r.item BETWEEN 100 AND 120
LIMIT 10 FORMAT Null SETTINGS log_comment = '05407_min_rows_in_view_function';

SYSTEM FLUSH LOGS query_log;

-- Each query is compared with the same query without the setting.
SELECT q.log_comment,
    q.ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] AS plan_built,
    q.ProfileEvents['AutoParallelReplicasPlanWithoutParallelReplicas'] AS plan_without_parallel_replicas,
    q.ProfileEvents['IndexAnalysisRounds'] = base.ProfileEvents['IndexAnalysisRounds'] AS same_index_analysis
FROM system.query_log AS q, system.query_log AS base
WHERE q.current_database = currentDatabase() AND base.current_database = currentDatabase()
    AND q.type = 'QueryFinish' AND base.type = 'QueryFinish' AND q.event_date >= yesterday() AND base.event_date >= yesterday()
    AND q.query_id = q.initial_query_id AND base.query_id = base.initial_query_id
    AND (q.log_comment, base.log_comment) IN (
        ('05407_without_min_rows', '05407_without_min_rows'),
        ('05407_min_rows_in_query', '05407_without_min_rows'),
        ('05407_min_rows_in_session', '05407_without_min_rows'),
        ('05407_subquery_without_min_rows', '05407_subquery_without_min_rows'),
        ('05407_min_rows_in_subquery', '05407_subquery_without_min_rows'),
        ('05407_view_without_min_rows', '05407_view_without_min_rows'),
        ('05407_min_rows_in_view', '05407_view_without_min_rows'),
        ('05407_view_function_without_min_rows', '05407_view_function_without_min_rows'),
        ('05407_min_rows_in_view_function', '05407_view_function_without_min_rows'))
ORDER BY q.event_time_microseconds;
