#!/usr/bin/env bash
# A plan-based parallel replicas plan splits the aggregation into the partial one and the merge, and ships the partial
# one to the replicas. Both the local and the shipped partial aggregation must get the `GROUP BY` top-K heap of the
# single-node aggregation: automatic parallel replicas matches the two plans, and prices the output of each replica by
# the output of the single-node aggregation. Every replica keeps its own `n` groups, so the cost model must not divide
# that output between the replicas, the same as for `LIMIT n`. The same for `GROUP BY ... LIMIT` without `ORDER BY`.
# Query-based parallel replicas are priced and switched the same way.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The formula is read from the server logs the client receives, because `system.text_log` can lose a line.
server_logs_file="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_05480_server_logs.txt"
rm -f "$server_logs_file"
CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

$CLICKHOUSE_CLIENT_TRACE --server_logs_file="$server_logs_file" -q "
DROP TABLE IF EXISTS t_05480;
CREATE TABLE t_05480 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_05480 SELECT intHash64(number) % 100000, number FROM numbers(1000000);

SET parallel_replicas_plan_based = 1, parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    automatic_parallel_replicas_min_bytes_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET enable_analyzer = 1, query_plan_enable_optimizations = 1, enable_group_by_top_k_optimization = 1,
    max_rows_to_group_by = 0, exact_rows_before_limit = 0, query_plan_max_limit_for_top_k_optimization = 1000;
-- The replicas aggregate in order under these, which takes no heap.
SET enable_memory_bound_merging_of_aggregation_results = 0, optimize_aggregation_in_order = 0;
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0;
-- Statistics of a previous run can take the heap away from the aggregation.
SET collect_hash_table_stats_during_aggregation = 0;
-- The top-K boundary of a run can leave an entry in the query condition cache, so that the next run reads fewer rows,
-- which looks like changed data and recollects the statistics instead of pricing.
SET use_query_condition_cache = 0;

-- Without the local replica, the remote ones read everything, and skip rows in the heap. The groups are the same as
-- without parallel replicas.
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k LIMIT 3 SETTINGS enable_parallel_replicas = 0;
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k LIMIT 3
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 0,
    log_comment = 'q05480_remote_asc';
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k DESC LIMIT 3 SETTINGS enable_parallel_replicas = 0;
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k DESC LIMIT 3
SETTINGS enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 0, parallel_replicas_local_plan = 0,
    log_comment = 'q05480_remote_desc';

SET enable_parallel_replicas = 1, parallel_replicas_local_plan = 1;

-- Mode 2 only matches the plans.
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k LIMIT 10
FORMAT Null SETTINGS automatic_parallel_replicas_mode = 2, enable_group_by_top_k_dynamic_filtering = 0,
    log_comment = 'q05480_match_asc';
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k DESC LIMIT 10
FORMAT Null SETTINGS automatic_parallel_replicas_mode = 2, enable_group_by_top_k_dynamic_filtering = 0,
    log_comment = 'q05480_match_desc';
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k LIMIT 10
FORMAT Null SETTINGS automatic_parallel_replicas_mode = 2, enable_group_by_top_k_dynamic_filtering = 1,
    log_comment = 'q05480_match_asc_dynamic_filtering';
SELECT k, count() FROM t_05480 GROUP BY k LIMIT 10
FORMAT Null SETTINGS automatic_parallel_replicas_mode = 2, log_comment = 'q05480_match_no_order_by';

-- Mode 1 prices the queries with the statistics that mode 2 collected above, and switches to parallel replicas: the
-- read is too small to occupy the threads, which would decide it before the cost model. The groups are the same as
-- without parallel replicas.
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k LIMIT 10 SETTINGS enable_parallel_replicas = 0;
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k LIMIT 10
SETTINGS automatic_parallel_replicas_mode = 1, automatic_parallel_replicas_ignore_thresholds = 1,
    enable_group_by_top_k_dynamic_filtering = 1, log_comment = 'q05480_priced_order_by';
SELECT k, count() FROM t_05480 GROUP BY k LIMIT 10
SETTINGS automatic_parallel_replicas_mode = 1, automatic_parallel_replicas_ignore_thresholds = 1,
    log_comment = 'q05480_priced_no_order_by';
-- The same with query-based parallel replicas.
SELECT k, count() FROM t_05480 GROUP BY k ORDER BY k LIMIT 10
SETTINGS automatic_parallel_replicas_mode = 1, automatic_parallel_replicas_ignore_thresholds = 1,
    parallel_replicas_plan_based = 0, log_comment = 'q05480_priced_query_based_order_by';
SELECT k, count() FROM t_05480 GROUP BY k LIMIT 10
SETTINGS automatic_parallel_replicas_mode = 1, automatic_parallel_replicas_ignore_thresholds = 1,
    parallel_replicas_plan_based = 0, log_comment = 'q05480_priced_query_based_no_order_by';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT q.log_comment, sum(r.ProfileEvents['AggregationTopKRowsSkipped']) > 0
FROM system.query_log AS r
INNER JOIN
(
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
        AND (current_database = currentDatabase()) AND startsWith(log_comment, 'q05480_remote_')
        AND (type = 'QueryFinish') AND is_initial_query
) AS q ON r.initial_query_id = q.query_id
WHERE (r.event_date >= yesterday()) AND (r.event_time >= NOW() - INTERVAL '15 MINUTES')
    AND (r.type = 'QueryFinish') AND NOT r.is_initial_query
GROUP BY q.log_comment
ORDER BY q.log_comment;

SELECT log_comment,
       ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable,
       ProfileEvents['AutoParallelReplicasNoStatistics'] AS no_statistics
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
    AND (current_database = currentDatabase()) AND startsWith(log_comment, 'q05480_match_')
    AND (type = 'QueryFinish') AND is_initial_query
ORDER BY log_comment;

-- Switched to parallel replicas, whose aggregations skip rows in the heap: the local one counts in the initial query.
SELECT q.log_comment, any(q.applied), sum(r.ProfileEvents['AggregationTopKRowsSkipped']) > 0
FROM system.query_log AS r
INNER JOIN
(
    SELECT query_id, log_comment, ProfileEvents['AutoParallelReplicasApplied'] AS applied
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
        AND (current_database = currentDatabase()) AND startsWith(log_comment, 'q05480_priced_')
        AND (type = 'QueryFinish') AND is_initial_query
) AS q ON r.initial_query_id = q.query_id
WHERE (r.event_date >= yesterday()) AND (r.event_time >= NOW() - INTERVAL '15 MINUTES') AND (r.type = 'QueryFinish')
GROUP BY q.log_comment
ORDER BY q.log_comment;

DROP TABLE t_05480;
"

# The divisor of the output bytes in `The applied formula: in / threads ? (in / threads + out / divisor)`.
$CLICKHOUSE_CLIENT -q "
    SELECT log_comment, query_id
    FROM system.query_log
    WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
        AND (current_database = currentDatabase()) AND startsWith(log_comment, 'q05480_priced_') AND (type = 'QueryFinish')
        AND is_initial_query
    ORDER BY log_comment" | while read -r log_comment query_id; do
    grep -F "{$query_id}" "$server_logs_file" | grep -F 'AutoParallelReplicas: The applied formula' \
        | sed -nE 's/.*\+ [0-9]+ \/ ([0-9]+)\).*/\1/p' | sed "s/^/$log_comment\t/"
done

rm -f "$server_logs_file"
