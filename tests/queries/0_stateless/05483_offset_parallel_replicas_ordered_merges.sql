-- Offset based parallel replicas: every replica of a shard returns its own part of the result, which the
-- initiator must still merge correctly when it relies on the order of the streams.

DROP TABLE IF EXISTS t_offset_pr_key;
DROP TABLE IF EXISTS t_offset_pr_sample;

CREATE TABLE t_offset_pr_key (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_offset_pr_key SELECT number FROM numbers(30000);

CREATE TABLE t_offset_pr_sample (k UInt64) ENGINE = MergeTree ORDER BY intHash32(k) SAMPLE BY intHash32(k);
INSERT INTO t_offset_pr_sample SELECT number FROM numbers(30000);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3, automatic_parallel_replicas_mode = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1, prefer_localhost_replica = 0;
-- Two-level results, merged bucket by bucket.
SET distributed_aggregation_memory_efficient = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1,
    enable_producing_buckets_out_of_order_in_aggregation = 0, optimize_aggregation_in_order = 0;

SELECT count(), sum(c), min(c), max(c)
FROM (SELECT k, count() AS c FROM cluster('test_cluster_two_shard_three_replicas_localhost', currentDatabase(), t_offset_pr_key) GROUP BY k)
SETTINGS parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'k';

SELECT count(), sum(c), min(c), max(c)
FROM (SELECT k, count() AS c FROM cluster('test_cluster_two_shard_three_replicas_localhost', currentDatabase(), t_offset_pr_key) GROUP BY k)
SETTINGS parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'cityHash64(k)';

SELECT count(), sum(c), min(c), max(c)
FROM (SELECT k, count() AS c FROM remote('127.0.0.{1|2|3}', currentDatabase(), t_offset_pr_sample) GROUP BY k)
SETTINGS parallel_replicas_mode = 'sampling_key';

SELECT k FROM remote('127.0.0.{1|2|3}', currentDatabase(), t_offset_pr_sample) ORDER BY k LIMIT 10
SETTINGS parallel_replicas_mode = 'sampling_key';

SELECT k FROM cluster('test_cluster_two_shard_three_replicas_localhost', currentDatabase(), t_offset_pr_key) ORDER BY k LIMIT 10
SETTINGS parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'k';

-- Without GROUP BY keys the parts are merged in any order, so the replicas still share the shard.
SELECT count() FROM remote('127.0.0.{1|2|3}', currentDatabase(), t_offset_pr_sample)
SETTINGS parallel_replicas_mode = 'sampling_key', use_hedged_requests = 0, log_queries = 1, log_comment = 'offset_pr_count';

SELECT count() FROM (SELECT k FROM remote('127.0.0.{1|2|3}', currentDatabase(), t_offset_pr_sample) GROUP BY k)
SETTINGS parallel_replicas_mode = 'sampling_key', use_hedged_requests = 0, log_queries = 1, log_comment = 'offset_pr_group_by';

-- Merged all at once instead of bucket by bucket, so the replicas still share the shard.
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM remote('127.0.0.{1|2|3}', currentDatabase(), t_offset_pr_sample) GROUP BY k)
SETTINGS parallel_replicas_mode = 'sampling_key', distributed_aggregation_memory_efficient = 0,
    enable_memory_bound_merging_of_aggregation_results = 0, use_hedged_requests = 0, log_queries = 1, log_comment = 'offset_pr_plain_merge';

-- With a local shard and aggregation in order the states are merged in order, so the remote shard is read from one replica.
SELECT k, count() FROM cluster('test_cluster_two_shard_three_replicas_localhost', currentDatabase(), t_offset_pr_key) GROUP BY k
SETTINGS parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'k', prefer_localhost_replica = 1,
    optimize_aggregation_in_order = 1, distributed_aggregation_memory_efficient = 0, enable_memory_bound_merging_of_aggregation_results = 1,
    use_hedged_requests = 0, log_queries = 1, log_comment = 'offset_pr_memory_bound'
FORMAT Null;

-- A full sort and an aggregation that is not in order take the parts in any order too.
SELECT k FROM (SELECT k FROM remote('127.0.0.{1|2|3}', currentDatabase(), t_offset_pr_sample)) ORDER BY k LIMIT 10
SETTINGS parallel_replicas_mode = 'sampling_key', use_hedged_requests = 0, log_queries = 1, log_comment = 'offset_pr_full_sort';

SELECT sum(k) FROM (SELECT k FROM remote('127.0.0.{1|2|3}', currentDatabase(), t_offset_pr_sample))
SETTINGS parallel_replicas_mode = 'sampling_key', use_hedged_requests = 0, log_queries = 1, log_comment = 'offset_pr_aggregating';

-- Expressions, filters and unions between the read and such a consumer are looked through.
SELECT sum(k % 2) FROM (SELECT k FROM remote('127.0.0.{1|2|3}', currentDatabase(), t_offset_pr_sample))
SETTINGS parallel_replicas_mode = 'sampling_key', use_hedged_requests = 0, log_queries = 1, log_comment = 'offset_pr_expression';

SELECT sum(k) FROM (SELECT k FROM remote('127.0.0.{1|2|3}', currentDatabase(), t_offset_pr_sample)) WHERE k % 2 = 0
SETTINGS parallel_replicas_mode = 'sampling_key', use_hedged_requests = 0, log_queries = 1, log_comment = 'offset_pr_filter';

SELECT sum(k) FROM
(
    SELECT k FROM remote('127.0.0.{1|2|3}', currentDatabase(), t_offset_pr_sample)
    UNION ALL
    SELECT k FROM remote('127.0.0.{1|2|3}', currentDatabase(), t_offset_pr_sample)
)
SETTINGS parallel_replicas_mode = 'sampling_key', use_hedged_requests = 0, log_queries = 1, log_comment = 'offset_pr_union';

-- Any other consumer, and the plan root, read a shard from one replica.
-- The limit is above the row count, so no secondary query is cancelled.
SELECT k FROM remote('127.0.0.{1|2|3}', currentDatabase(), t_offset_pr_sample) LIMIT 100000
SETTINGS parallel_replicas_mode = 'sampling_key', use_hedged_requests = 0, log_queries = 1, log_comment = 'offset_pr_limit'
FORMAT Null;

SELECT k FROM remote('127.0.0.{1|2|3}', currentDatabase(), t_offset_pr_sample)
SETTINGS parallel_replicas_mode = 'sampling_key', use_hedged_requests = 0, log_queries = 1, log_comment = 'offset_pr_root'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT initial.log_comment, count()
FROM system.query_log AS secondary
INNER JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish'
        AND log_comment IN ('offset_pr_count', 'offset_pr_group_by', 'offset_pr_plain_merge', 'offset_pr_full_sort', 'offset_pr_aggregating', 'offset_pr_expression', 'offset_pr_filter', 'offset_pr_union', 'offset_pr_limit', 'offset_pr_root', 'offset_pr_memory_bound')
) AS initial ON secondary.initial_query_id = initial.query_id
WHERE secondary.event_date >= yesterday() AND NOT secondary.is_initial_query AND secondary.type = 'QueryFinish'
GROUP BY initial.log_comment
ORDER BY initial.log_comment
SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_offset_pr_sample;
DROP TABLE t_offset_pr_key;
