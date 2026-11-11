-- Each of `AutoParallelReplicasPlansNotComparable` and `AutoParallelReplicasStatisticsDrifted` must be
-- counted when its case happens, not only stay zero when it does not.

DROP TABLE IF EXISTS t_autopr_not_comparable;
DROP TABLE IF EXISTS t_autopr_drifted;

CREATE TABLE t_autopr_not_comparable (k UInt64, v UInt64, PROJECTION p (SELECT k % 10, sum(v) GROUP BY k % 10))
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_autopr_not_comparable SELECT number, number FROM numbers(100000);

CREATE TABLE t_autopr_drifted (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_autopr_drifted SELECT number, number FROM numbers(100000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    automatic_parallel_replicas_min_bytes_per_replica = 0, serialize_query_plan = 0;
-- An aggregation that spills to disk merges on a path that collects no statistics, so the first run of the
-- drift case would leave nothing for the second one to drift from.
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

-- Not comparable: the single-node plan reads the projection, while the plan with parallel replicas is built
-- without projections, so the two have no node in common to collect statistics at.
SELECT k % 10, sum(v) FROM t_autopr_not_comparable GROUP BY k % 10
FORMAT Null SETTINGS optimize_use_projections = 1, log_comment = 'events_not_comparable';

-- Drifted: the first run collects statistics, then the table grows past twice the rows they were measured
-- on, so the second run recollects instead of deciding.
SELECT k % 10, sum(v) FROM t_autopr_drifted GROUP BY k % 10 FORMAT Null SETTINGS log_comment = 'events_drift_1_collect';
INSERT INTO t_autopr_drifted SELECT number + 100000, number FROM numbers(300000);
SELECT k % 10, sum(v) FROM t_autopr_drifted GROUP BY k % 10 FORMAT Null SETTINGS log_comment = 'events_drift_2_drifted';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] AS built,
       ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable,
       ProfileEvents['AutoParallelReplicasNoStatistics'] AS no_statistics,
       ProfileEvents['AutoParallelReplicasStatisticsDrifted'] AS drifted
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  -- `events_` cannot collide with the comment the test harness sets, which starts with the test name
  AND log_comment LIKE 'events_%'
  -- the replicas run sub-queries under the same comment; keep only the initiating query
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE t_autopr_not_comparable;
DROP TABLE t_autopr_drifted;
