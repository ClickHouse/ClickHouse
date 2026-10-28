-- Automatic parallel replicas collects the CPU time of the replicas' share of the work along with its bytes,
-- split the way parallel replicas would split it: the work over the coordinated read is divided between the
-- replicas, while the work over every other read - here the build side of a join - is repeated by each.

DROP TABLE IF EXISTS probe_side_05453;
DROP TABLE IF EXISTS build_side_05453;

CREATE TABLE probe_side_05453 (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE build_side_05453 (id UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO probe_side_05453 SELECT number FROM numbers(1000000);
INSERT INTO build_side_05453 SELECT number FROM numbers(600000);

-- Mode 2 only collects statistics, on every execution.
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1,
    automatic_parallel_replicas_mode = 2;
SET serialize_query_plan = 0;
-- Keep the build side on the right and unfiltered, so that the probe side is the coordinated read.
SET query_plan_join_swap_table = 0, enable_join_runtime_filters = 0,
    query_plan_optimize_join_order_randomize = 0;

SELECT id % 1000 AS k, count() FROM probe_side_05453 GROUP BY k
FORMAT Null SETTINGS log_comment = 'cpu_time_1_aggregation';

SELECT sum(p.id + b.id) FROM probe_side_05453 AS p INNER JOIN build_side_05453 AS b ON p.id = b.id
FORMAT Null SETTINGS log_comment = 'cpu_time_2_join';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['RuntimeDataflowStatisticsCPUNanoseconds'] > 0 AS split_cpu,
       ProfileEvents['RuntimeDataflowStatisticsDuplicatedCPUNanoseconds'] > 0 AS duplicated_cpu
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND log_comment LIKE 'cpu_time_%' AND type = 'QueryFinish'
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE probe_side_05453;
DROP TABLE build_side_05453;
