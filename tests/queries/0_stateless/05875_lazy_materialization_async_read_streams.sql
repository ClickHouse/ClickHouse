-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test counts the readers of the lazy read on the initiator.

-- With `allow_asynchronous_read_from_io_pool_for_merge_tree`, the lazy read uses as many streams as
-- `max_streams_for_merge_tree_reading` allows, like the main read, so the 10 rows in 10 granules get 10 readers.

SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, use_query_condition_cache = 0,
    query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000;

DROP TABLE IF EXISTS t_lazy_async_streams;
CREATE TABLE t_lazy_async_streams (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_lazy_async_streams SELECT number, toString(number) FROM numbers(100000) SETTINGS max_insert_threads = 1;

SELECT groupBitXor(cityHash64(k, s)), count() FROM (SELECT k, s FROM t_lazy_async_streams ORDER BY cityHash64(k) LIMIT 10)
SETTINGS allow_asynchronous_read_from_io_pool_for_merge_tree = 1, max_streams_for_merge_tree_reading = 16,
    log_processors_profiles = 1, log_comment = '05875_async_16';
SELECT groupBitXor(cityHash64(k, s)), count() FROM (SELECT k, s FROM t_lazy_async_streams ORDER BY cityHash64(k) LIMIT 10)
SETTINGS allow_asynchronous_read_from_io_pool_for_merge_tree = 0, max_streams_for_merge_tree_reading = 16,
    log_processors_profiles = 1, log_comment = '05875_sync_16';
SELECT groupBitXor(cityHash64(k, s)), count() FROM (SELECT k, s FROM t_lazy_async_streams ORDER BY cityHash64(k) LIMIT 10)
SETTINGS allow_asynchronous_read_from_io_pool_for_merge_tree = 1, max_streams_for_merge_tree_reading = 2,
    log_processors_profiles = 1, log_comment = '05875_async_2';
SELECT groupBitXor(cityHash64(k, s)), count() FROM (SELECT k, s FROM t_lazy_async_streams ORDER BY cityHash64(k) LIMIT 10)
SETTINGS allow_asynchronous_read_from_io_pool_for_merge_tree = 1, max_streams_for_merge_tree_reading = 16, max_threads = 1,
    log_processors_profiles = 1, log_comment = '05875_async_16_one_thread';
SELECT groupBitXor(cityHash64(k, s)), count() FROM (SELECT k, s FROM t_lazy_async_streams ORDER BY cityHash64(k) LIMIT 10)
SETTINGS allow_asynchronous_read_from_io_pool_for_merge_tree = 1, max_streams_for_merge_tree_reading = 0,
    max_streams_to_max_threads_ratio = 4, log_processors_profiles = 1, log_comment = '05875_async_ratio_only';

SYSTEM FLUSH LOGS query_log, processors_profile_log;

SELECT q.log_comment, countIf(p.plan_step_name = 'LazilyReadFromMergeTree' AND startsWith(p.name, 'MergeTreeSelect')) AS readers
FROM system.query_log AS q
INNER JOIN system.processors_profile_log AS p ON p.query_id = q.query_id
WHERE q.event_date >= yesterday() AND q.event_time >= now() - INTERVAL 10 MINUTE
    AND p.event_date >= yesterday() AND p.event_time >= now() - INTERVAL 10 MINUTE
    AND q.current_database = currentDatabase() AND q.type = 'QueryFinish'
    AND q.log_comment IN ('05875_async_16', '05875_sync_16', '05875_async_2', '05875_async_16_one_thread', '05875_async_ratio_only')
GROUP BY q.log_comment
ORDER BY q.log_comment;

DROP TABLE t_lazy_async_streams;
