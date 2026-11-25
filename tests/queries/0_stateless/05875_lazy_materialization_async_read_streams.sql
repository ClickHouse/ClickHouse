-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test counts the readers of the lazy read on the initiator.

-- With `allow_asynchronous_read_from_io_pool_for_merge_tree`, the lazy read uses as many streams as
-- `max_streams_for_merge_tree_reading` allows, like the main read, so the 10 rows in 10 granules get 10 readers.

SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, use_query_condition_cache = 0,
    query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000;

DROP TABLE IF EXISTS t_lazy_async_streams;
CREATE TABLE t_lazy_async_streams (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_lazy_async_streams SELECT number, toString(number) FROM numbers(100000);

SELECT groupBitXor(cityHash64(k, s)), count() FROM (SELECT k, s FROM t_lazy_async_streams ORDER BY cityHash64(k) LIMIT 10)
SETTINGS allow_asynchronous_read_from_io_pool_for_merge_tree = 1, max_streams_for_merge_tree_reading = 16,
    log_processors_profiles = 1, log_comment = '05875_async_16';
SELECT groupBitXor(cityHash64(k, s)), count() FROM (SELECT k, s FROM t_lazy_async_streams ORDER BY cityHash64(k) LIMIT 10)
SETTINGS allow_asynchronous_read_from_io_pool_for_merge_tree = 0, max_streams_for_merge_tree_reading = 16,
    log_processors_profiles = 1, log_comment = '05875_sync_16';
SELECT groupBitXor(cityHash64(k, s)), count() FROM (SELECT k, s FROM t_lazy_async_streams ORDER BY cityHash64(k) LIMIT 10)
SETTINGS allow_asynchronous_read_from_io_pool_for_merge_tree = 1, max_streams_for_merge_tree_reading = 2,
    log_processors_profiles = 1, log_comment = '05875_async_2';

SYSTEM FLUSH LOGS query_log, processors_profile_log;

WITH
    (
        SELECT groupArray((log_comment, readers))
        FROM
        (
            SELECT q.log_comment AS log_comment, countIf(p.plan_step_name = 'LazilyReadFromMergeTree' AND startsWith(p.name, 'MergeTreeSelect')) AS readers
            FROM system.query_log AS q
            INNER JOIN system.processors_profile_log AS p ON p.query_id = q.query_id
            WHERE q.event_date >= yesterday() AND p.event_date >= yesterday() AND q.current_database = currentDatabase()
                AND q.type = 'QueryFinish' AND q.log_comment LIKE '05875_%'
            GROUP BY log_comment
        )
    ) AS counts,
    arrayFirst(x -> x.1 = '05875_async_16', counts).2 AS async_16,
    arrayFirst(x -> x.1 = '05875_sync_16', counts).2 AS sync_16,
    arrayFirst(x -> x.1 = '05875_async_2', counts).2 AS async_2
SELECT
    'async_16 vs sync_16', multiIf(async_16 > sync_16, 'more', async_16 = sync_16, 'same', 'fewer'),
    'async_2 vs sync_16', multiIf(async_2 > sync_16, 'more', async_2 = sync_16, 'same', 'fewer'),
    sync_16 > 0;

DROP TABLE t_lazy_async_streams;
