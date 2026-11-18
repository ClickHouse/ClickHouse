-- Read limits under plan-based parallel replicas (`parallel_replicas_plan_based`) apply to the total read across
-- replicas, not only to the share of each replica: the initiator checks the progress the remote replicas report.
-- https://github.com/ClickHouse/ClickHouse/issues/132639

DROP TABLE IF EXISTS t_pr_storage_limits;

CREATE TABLE t_pr_storage_limits (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 128;
-- 8 MB of `x`.
INSERT INTO t_pr_storage_limits SELECT number FROM numbers(1000000);

SET enable_parallel_replicas = 1;
SET parallel_replicas_plan_based = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_threads = 1;

-- The limit is just below the size of the table. Without the local plan every replica is a remote one, and the read
-- is shared between them, so no replica alone reaches the limit: only the check of the total on the initiator does.
SELECT count() FROM t_pr_storage_limits WHERE x % 7 = 1
SETTINGS parallel_replicas_local_plan = 0, max_bytes_to_read = 7500000; -- { serverError TOO_MANY_BYTES }
-- With the local plan, the initiator checks the total of the local read and of the remote replicas.
SELECT count() FROM t_pr_storage_limits WHERE x % 7 = 1
SETTINGS parallel_replicas_local_plan = 1, max_bytes_to_read = 7500000; -- { serverError TOO_MANY_BYTES }

DROP TABLE t_pr_storage_limits;
