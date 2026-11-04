-- Direct read from a text index rewrites the text-search functions of a read into virtual columns, taking
-- the conditions from the index analysis of that read. The plan for automatic parallel replicas is optimized
-- without that analysis, so its read kept `hasToken`, its steps hashed differently from the single-node
-- plan's, and the two plans never matched.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx_str str TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 8
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO tab SELECT number, concat('common w', toString(number % 50), if(number % 1000 = 7, ' rare', '')) FROM numbers(100000);

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
-- The local branch is optimized on its own only in this implementation.
SET parallel_replicas_plan_based = 0;
SET enable_parallel_replicas = 1;
-- Mode 2 always collects the statistics, without consulting the cost model.
SET automatic_parallel_replicas_mode = 2;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

SELECT id % 10 AS k, count() FROM tab WHERE hasToken(str, 'rare') GROUP BY k ORDER BY k
SETTINGS log_comment = '05507_direct_read';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

-- The query has to use direct read, or the check below passes without testing anything.
SELECT count() > 0
FROM (EXPLAIN actions = 1 SELECT id % 10 AS k, count() FROM tab WHERE hasToken(str, 'rare') GROUP BY k ORDER BY k)
WHERE explain LIKE '%__text_index_idx_str%';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable,
    ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS collected_statistics
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
  AND event_date >= yesterday() AND log_comment = '05507_direct_read'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE tab;
