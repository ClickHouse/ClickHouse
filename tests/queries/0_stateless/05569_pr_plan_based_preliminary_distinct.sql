-- Plan-based parallel replicas ships the preliminary `DISTINCT` with the plan fragment, so every replica
-- sends only the distinct rows of its share instead of every row it reads, and can read in order for it.
-- https://github.com/ClickHouse/ClickHouse/issues/128560

DROP TABLE IF EXISTS t_pr_preliminary_distinct;

CREATE TABLE t_pr_preliminary_distinct (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO t_pr_preliminary_distinct SELECT number % 100, number % 10, number FROM numbers(200000);

SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;
-- Otherwise the local replica can read everything before a remote one gets any work.
SET parallel_replicas_local_plan = 0;

SELECT count() FROM (SELECT DISTINCT a, b FROM t_pr_preliminary_distinct) SETTINGS log_comment = '05569_distinct';

SYSTEM FLUSH LOGS query_log;

-- Rows sent by each replica.
SELECT max(result_rows) < 10000
FROM system.query_log
WHERE event_date >= yesterday() AND NOT is_initial_query AND type = 'QueryFinish'
    AND initial_query_id IN
    (
        SELECT query_id
        FROM system.query_log
        WHERE current_database = currentDatabase() AND event_date >= yesterday() AND is_initial_query AND type = 'QueryFinish'
            AND log_comment = '05569_distinct'
    );

-- With a LIMIT, a final DISTINCT and the LIMIT are shipped too, so a replica sends at most 3 rows.
SELECT arrayJoin(extractAll(explain, 'Limit \\([^)]+\\)|Distinct \\([^)]+\\)|ReadFromParallelReplicas|ReadFromMergeTree'))
FROM (EXPLAIN SELECT DISTINCT c FROM t_pr_preliminary_distinct LIMIT 3);

SELECT count() FROM (SELECT DISTINCT c FROM t_pr_preliminary_distinct LIMIT 3);

-- The local replica reads in order for the `DISTINCT`.
SELECT count() > 0
FROM (EXPLAIN PIPELINE SELECT DISTINCT a, b FROM t_pr_preliminary_distinct
    SETTINGS parallel_replicas_local_plan = 1, optimize_distinct_in_order = 1, optimize_read_in_order = 1)
WHERE explain LIKE '%DistinctSortedStreamTransform%';

DROP TABLE t_pr_preliminary_distinct;
