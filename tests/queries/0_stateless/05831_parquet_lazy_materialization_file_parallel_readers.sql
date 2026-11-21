-- Tags: no-fasttest
-- no-fasttest: reads Parquet files

-- Lazy materialization reads the deferred columns of the local Parquet files with surviving rows by up to `max_threads`
-- readers, each over a contiguous group of files, and returns their chunks in group order. The readers together read no
-- more leaf columns at once than the main pass, so a query that reads many columns lazily uses fewer readers.

-- `max_threads` is lowered neither when the server is short of free memory nor to the fair share of CPU slots of a busy
-- server (`max_threads = 16` is `auto` on a server with 16 cores); top-K filtering does not change which files have rows.
SET engine_file_truncate_on_insert = 1, query_plan_optimize_lazy_materialization = 1, query_plan_optimize_lazy_materialization_for_file = 1,
    query_plan_max_limit_for_lazy_materialization = 0, max_threads_min_free_memory_per_thread = 0, use_concurrency_control_for_max_threads = 0,
    use_top_k_dynamic_filtering = 0, max_streams_to_max_threads_ratio = 1;

-- Eight files of 2000 rows.
INSERT INTO FUNCTION file(concat(currentDatabase(), '_lazy_parallel/data_{_partition_id}.parquet'), Parquet) PARTITION BY intDiv(k, 2000)
SELECT number AS k, cityHash64(number) % 1000003 AS ts, concat('val_', toString(number)) AS s, toString(number) AS c1,
    concat('a', c1) AS c2, concat('b', c1) AS c3, concat('c', c1) AS c4, concat('d', c1) AS c5,
    CAST((c1, c2, c3, c4, c5), 'Tuple(c1 String, c2 String, c3 String, c4 String, c5 String)') AS t,
    CAST(s, 'Variant(String, UInt64)') AS v
FROM numbers(16000);

SELECT '-- rows and files of the LIMITs';
SELECT count(), uniqExact(_file) FROM (SELECT k, _file FROM file(concat(currentDatabase(), '_lazy_parallel/data_*.parquet'), Parquet) ORDER BY ts, k LIMIT 3000);
SELECT count(), uniqExact(_file) FROM (SELECT k, _file FROM file(concat(currentDatabase(), '_lazy_parallel/data_*.parquet'), Parquet) ORDER BY ts, k LIMIT 100);

-- Small blocks: every reader returns many chunks, so the chunks of later groups arrive before earlier groups finish.
SELECT '-- every lazily read value belongs to its row';
SELECT count(), sum(k), countIf(s != concat('val_', toString(k)))
FROM (SELECT k, s FROM file(concat(currentDatabase(), '_lazy_parallel/data_*.parquet'), Parquet) ORDER BY ts, k LIMIT 3000)
SETTINGS max_threads = 1, max_block_size = 7;
SELECT count(), sum(k), countIf(s != concat('val_', toString(k)))
FROM (SELECT k, s FROM file(concat(currentDatabase(), '_lazy_parallel/data_*.parquet'), Parquet) ORDER BY ts, k LIMIT 3000)
SETTINGS max_threads = 4, max_block_size = 7;
SELECT count(), sum(k), countIf(s != concat('val_', toString(k)))
FROM (SELECT k, s FROM file(concat(currentDatabase(), '_lazy_parallel/data_*.parquet'), Parquet) ORDER BY ts, k LIMIT 3000)
SETTINGS max_threads = 16, max_block_size = 7;
SELECT count(), sum(k), countIf(s != concat('val_', toString(k)))
FROM (SELECT k, s FROM file(concat(currentDatabase(), '_lazy_parallel/data_*.parquet'), Parquet) ORDER BY ts, k LIMIT 3000)
SETTINGS max_threads = 16, max_block_size = 7, query_plan_optimize_lazy_materialization_for_file = 0;
SELECT count(), countIf(c5 != concat('d', toString(k)))
FROM (SELECT k, c1, c2, c3, c4, c5 FROM file(concat(currentDatabase(), '_lazy_parallel/data_*.parquet'), Parquet) ORDER BY ts, k LIMIT 100)
SETTINGS max_threads = 16, max_block_size = 7;

-- The main pass reads `ts` and `k` of 8 files; the lazy pass reads 1 leaf column (narrow) or 5 (wide, and the Tuple of
-- 5 elements read as the values of an `AggregateFunction` state). The leaf columns of a `JSON` or `Variant` column depend on the file.
SELECT k, s FROM file(concat(currentDatabase(), '_lazy_parallel/data_*.parquet'), Parquet) ORDER BY ts, k LIMIT 3000
FORMAT Null SETTINGS max_threads = 4, log_processors_profiles = 1, log_comment = 'readers_04_narrow';
SELECT k, s FROM file(concat(currentDatabase(), '_lazy_parallel/data_*.parquet'), Parquet) ORDER BY ts, k LIMIT 3000
FORMAT Null SETTINGS max_threads = 16, log_processors_profiles = 1, log_comment = 'readers_16_narrow';
SELECT k, c1, c2, c3, c4, c5 FROM file(concat(currentDatabase(), '_lazy_parallel/data_*.parquet'), Parquet) ORDER BY ts, k LIMIT 100
FORMAT Null SETTINGS max_threads = 16, log_processors_profiles = 1, log_comment = 'readers_16_wide';
SELECT k, t FROM file(concat(currentDatabase(), '_lazy_parallel/data_*.parquet'), Parquet, 'k Int64, ts Int64, t JSON') ORDER BY ts, k LIMIT 100
FORMAT Null SETTINGS max_threads = 16, log_processors_profiles = 1, log_comment = 'readers_16_json';
SELECT k, t
FROM file(concat(currentDatabase(), '_lazy_parallel/data_*.parquet'), Parquet,
    'k Int64, ts Int64, t AggregateFunction(any, Tuple(c1 String, c2 String, c3 String, c4 String, c5 String))')
ORDER BY ts, k LIMIT 100
FORMAT Null SETTINGS max_threads = 16, aggregate_function_input_format = 'value', log_processors_profiles = 1, log_comment = 'readers_16_aggregate_value';
-- The ratio lowers the stream budget of the main read, and the lazy readers follow it.
SELECT k, s FROM file(concat(currentDatabase(), '_lazy_parallel/data_*.parquet'), Parquet) ORDER BY ts, k LIMIT 3000
FORMAT Null SETTINGS max_threads = 16, max_streams_to_max_threads_ratio = 0.25, log_processors_profiles = 1, log_comment = 'readers_16_narrow_streams';
SELECT k, v FROM file(concat(currentDatabase(), '_lazy_parallel/data_*.parquet'), Parquet, 'k Int64, ts Int64, v Variant(String, UInt64)') ORDER BY ts, k LIMIT 3000
FORMAT Null SETTINGS max_threads = 16, log_processors_profiles = 1, log_comment = 'readers_16_variant';

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
