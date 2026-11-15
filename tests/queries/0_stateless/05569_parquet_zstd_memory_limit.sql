-- Tags: no-fasttest

INSERT INTO FUNCTION file(currentDatabase() || '_zstd_level.parquet', Parquet)
SELECT number AS a, a + 1 AS b, a + 2 AS c, a + 3 AS d FROM numbers(100000)
SETTINGS output_format_compression_level = 22, max_threads = 2, log_comment = 'parquet_zstd_level_22';

SYSTEM FLUSH LOGS query_log;
SELECT memory_usage < 500000000 FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'parquet_zstd_level_22' AND type = 'QueryFinish';
