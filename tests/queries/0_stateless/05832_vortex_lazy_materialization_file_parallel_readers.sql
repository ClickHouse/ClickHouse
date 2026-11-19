-- Tags: no-fasttest, no-msan
-- no-fasttest, no-msan: the Vortex format is not included in the fast test and MSan builds

-- Vortex reads whole top-level fields and converts them to the requested types, so the width of a lazy read of Vortex
-- files is unknown and it keeps one reader (see 05831_parquet_lazy_materialization_file_parallel_readers).

SET engine_file_truncate_on_insert = 1, query_plan_optimize_lazy_materialization = 1, query_plan_optimize_lazy_materialization_for_file = 1,
    query_plan_max_limit_for_lazy_materialization = 0, max_threads_min_free_memory_per_thread = 0, use_concurrency_control_for_max_threads = 0,
    use_top_k_dynamic_filtering = 0, max_streams_to_max_threads_ratio = 1;

-- Eight files of 2000 rows.
INSERT INTO FUNCTION file(concat(currentDatabase(), '_lazy_parallel_vortex/data_{_partition_id}.vortex'), Vortex) PARTITION BY intDiv(k, 2000)
SELECT number AS k, cityHash64(number) % 1000003 AS ts, concat('val_', toString(number)) AS s FROM numbers(16000);

SELECT '-- every lazily read value belongs to its row';
SELECT count(), sum(k), countIf(s != concat('val_', toString(k)))
FROM (SELECT k, s FROM file(concat(currentDatabase(), '_lazy_parallel_vortex/data_*.vortex'), Vortex) ORDER BY ts, k LIMIT 3000)
SETTINGS max_threads = 16, max_block_size = 7;

SELECT k, s FROM file(concat(currentDatabase(), '_lazy_parallel_vortex/data_*.vortex'), Vortex) ORDER BY ts, k LIMIT 3000
FORMAT Null SETTINGS max_threads = 16, log_processors_profiles = 1, log_comment = 'readers_16_narrow';

SELECT '-- the number of readers';
SYSTEM FLUSH LOGS query_log, processors_profile_log;
SELECT q.log_comment, countIf(p.name = 'StorageFileLazyRows')
FROM system.query_log AS q
LEFT JOIN (
    SELECT query_id, name FROM system.processors_profile_log
    WHERE event_date >= yesterday() AND name = 'StorageFileLazyRows'
        AND query_id IN (SELECT query_id FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase()
            AND type = 'QueryFinish' AND startsWith(log_comment, 'readers_'))
) AS p ON p.query_id = q.query_id
WHERE q.event_date >= yesterday() AND q.current_database = currentDatabase() AND q.type = 'QueryFinish' AND startsWith(q.log_comment, 'readers_')
GROUP BY q.log_comment
ORDER BY q.log_comment;
