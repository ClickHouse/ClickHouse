-- With `parallel_replicas_index_analysis_only_on_coordinator`, a follower skips its own index analysis and relies on the
-- one done by the initiator's local plan. Without that local plan - parallel replicas inside a `Distributed` subquery,
-- or `parallel_replicas_prefer_local_replica = 0` - nobody analyzes, so the followers must analyze themselves rather
-- than read whole parts.

SET automatic_parallel_replicas_mode = 0;
SET use_query_condition_cache = 0;
SET max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_min_number_of_rows_per_replica = 0,
    parallel_replicas_local_plan = 1, parallel_replicas_index_analysis_only_on_coordinator = 1;

DROP TABLE IF EXISTS t_pr_no_local_plan;
DROP TABLE IF EXISTS t_pr_no_local_plan_d;

CREATE TABLE t_pr_no_local_plan (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_pr_no_local_plan SELECT number FROM numbers(640000);

CREATE TABLE t_pr_no_local_plan_d AS t_pr_no_local_plan
ENGINE = Distributed('test_cluster_one_shard_three_replicas_localhost', currentDatabase(), t_pr_no_local_plan);

SELECT count(), sum(k) FROM t_pr_no_local_plan WHERE k BETWEEN 1000 AND 1100
SETTINGS enable_parallel_replicas = 0, log_comment = '05428_single';

SELECT sum(k) FROM t_pr_no_local_plan_d WHERE k BETWEEN 1000 AND 1100
SETTINGS enable_parallel_replicas = 1, prefer_localhost_replica = 0, log_comment = '05428_distributed';

SELECT sum(k) FROM t_pr_no_local_plan WHERE k BETWEEN 1000 AND 1100
SETTINGS enable_parallel_replicas = 1, parallel_replicas_prefer_local_replica = 0, log_comment = '05428_no_local_replica';

SYSTEM FLUSH LOGS query_log;

-- Some replica reads the data, so the maximum is positive; one that read whole parts would select all 10000 marks.
WITH (
    SELECT ProfileEvents['SelectedMarks'] FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND log_comment = '05428_single'
) AS single_marks
SELECT
    replaceOne(log_comment, '05428_', '') AS mode,
    max(ProfileEvents['SelectedMarks']) BETWEEN 1 AND single_marks
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND NOT is_initial_query
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query
            AND log_comment IN ('05428_distributed', '05428_no_local_replica'))
GROUP BY mode
ORDER BY mode;

DROP TABLE t_pr_no_local_plan_d;
DROP TABLE t_pr_no_local_plan;
