-- A bucketed distributed read is pinned to the marks the coordinator selected. A worker task does not
-- repeat the coordinator's index analysis: summed over the tasks, the selected marks equal those of the
-- same query on a single node, and no task analyzes the primary key. Only when the worker can prune more
-- than the coordinator, with an `IN` subquery set under `use_index_for_in_with_subqueries = 0` (see
-- 05021_distributed_plan_read_locally_pruned_part), does it analyze, and then only its own marks.

DROP TABLE IF EXISTS t_dp_marks;
DROP TABLE IF EXISTS t_dp_keys;
DROP TABLE IF EXISTS t_dp_final;

CREATE TABLE t_dp_marks (id UInt64, v UInt64, INDEX idx_v v TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1000, index_granularity_bytes = '10Mi';
INSERT INTO t_dp_marks SELECT number, intDiv(number, 1000) FROM numbers(100000);

CREATE TABLE t_dp_keys (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1000, index_granularity_bytes = '10Mi';
INSERT INTO t_dp_keys SELECT number FROM numbers(10000);

SET use_query_condition_cache = 0;
SET enable_parallel_replicas = 0;
-- `make_distributed_plan` turns it off, so turn it off for the control too: both then count the marks
-- selected by the index analysis.
SET use_skip_indexes_on_data_read = 0;

-- The primary key selects marks 0..30 and the skip index keeps marks 0..9 of them.
SELECT count(), sum(v) FROM t_dp_marks WHERE id < 30000 AND v < 10 -- dp_marks_control
SETTINGS make_distributed_plan = 0;

SELECT count(), sum(v) FROM t_dp_marks WHERE id < 30000 AND v < 10 -- dp_marks_distributed
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_fallback_to_local_execution = 0, enable_join_runtime_filters = 0;

-- The coordinator cannot use the set, so it selects all 100 marks; the workers prune their shares to the
-- marks that hold the keys, by the set they receive as values.
SELECT count(), sum(v) FROM t_dp_marks WHERE id IN (SELECT k FROM t_dp_keys) -- dp_marks_in_set
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_fallback_to_local_execution = 0, enable_join_runtime_filters = 0,
    use_index_for_in_with_subqueries = 0, max_rows_to_group_by = 0;

SYSTEM FLUSH LOGS query_log;

WITH
    (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND event_time > now() - INTERVAL 10 MINUTE
            AND current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
            AND query LIKE '%dp_marks_distributed%' AND query NOT LIKE '%query_log%'
        ORDER BY event_time_microseconds DESC LIMIT 1
    ) AS distributed_query,
    (
        SELECT ProfileEvents['SelectedMarks'] FROM system.query_log
        WHERE event_date >= yesterday() AND event_time > now() - INTERVAL 10 MINUTE
            AND current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
            AND query LIKE '%dp_marks_control%' AND query NOT LIKE '%query_log%'
        ORDER BY event_time_microseconds DESC LIMIT 1
    ) AS control_marks
SELECT
    'default',
    countIf(ProfileEvents['SelectedMarks'] > 0) AS reading_tasks,
    sum(ProfileEvents['SelectedMarks']) = control_marks AS marks_match_single_node,
    sum(ProfileEvents['FilteringMarksWithPrimaryKeyProcessedMarks']) AS pk_marks_analyzed_on_workers
FROM system.query_log
WHERE event_date >= yesterday() AND event_time > now() - INTERVAL 10 MINUTE
    AND type = 'QueryFinish' AND is_initial_query = 0 AND initial_query_id = distributed_query;

-- The read tasks analyze the 100 marks the coordinator sent them, not 100 marks each, and select the
-- marks of `t_dp_marks` that hold the keys. The 10 marks of `t_dp_keys`, which the set subquery reads
-- without a filter, count as selected too.
WITH
    (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND event_time > now() - INTERVAL 10 MINUTE
            AND current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
            AND query LIKE '%dp_marks_in_set%' AND query NOT LIKE '%query_log%'
        ORDER BY event_time_microseconds DESC LIMIT 1
    ) AS distributed_query
SELECT
    'in_set',
    sum(ProfileEvents['FilteringMarksWithPrimaryKeyProcessedMarks']) AS pk_marks_analyzed_on_workers,
    sum(ProfileEvents['SelectedMarks']) AS marks_selected_on_workers
FROM system.query_log
WHERE event_date >= yesterday() AND event_time > now() - INTERVAL 10 MINUTE
    AND type = 'QueryFinish' AND is_initial_query = 0 AND initial_query_id = distributed_query;

-- A FINAL read keeps all local parts, and resolves each lane's marks against them, but it does not
-- repeat the coordinator's index analysis either. Two parts with overlapping keys, so that the
-- primary-key-range layers merge rows of both.
CREATE TABLE t_dp_final (id UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY id
SETTINGS index_granularity = 1000, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_dp_final;
INSERT INTO t_dp_final SELECT number, number FROM numbers(50000);
INSERT INTO t_dp_final SELECT number, number + 1 FROM numbers(25000, 50000);

SELECT count(), sum(v) FROM t_dp_final FINAL WHERE id < 60000 SETTINGS make_distributed_plan = 0;

SELECT count(), sum(v) FROM t_dp_final FINAL WHERE id < 60000 -- dp_marks_final
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_fallback_to_local_execution = 0, enable_join_runtime_filters = 0,
    max_rows_to_group_by = 0;

SYSTEM FLUSH LOGS query_log;

WITH
    (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND event_time > now() - INTERVAL 10 MINUTE
            AND current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
            AND query LIKE '%dp_marks_final%' AND query NOT LIKE '%query_log%'
        ORDER BY event_time_microseconds DESC LIMIT 1
    ) AS distributed_query
SELECT
    'final',
    countIf(ProfileEvents['SelectedRows'] > 0) > 1 AS several_reading_tasks,
    sum(ProfileEvents['FilteringMarksWithPrimaryKeyProcessedMarks']) AS pk_marks_analyzed_on_workers
FROM system.query_log
WHERE event_date >= yesterday() AND event_time > now() - INTERVAL 10 MINUTE
    AND type = 'QueryFinish' AND is_initial_query = 0 AND initial_query_id = distributed_query;

DROP TABLE t_dp_final;
DROP TABLE t_dp_keys;
DROP TABLE t_dp_marks;
