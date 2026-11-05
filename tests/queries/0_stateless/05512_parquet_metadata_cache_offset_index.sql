-- Tags: no-fasttest, no-parallel-replicas
-- no-fasttest: depends on S3
-- no-parallel-replicas: profile events are not available on the other replicas
-- Random settings limits: use_columns_cache=(0, 0); use_query_condition_cache=(0, 0)

-- A read that finds the footer of a Parquet file in the metadata cache keeps the offset indexes it reads with it,
-- so that later reads of the file do not request them again. Every file is private to the test database, so the
-- cache is not dropped. A column served from the columns cache, or a read skipped by the query condition cache,
-- reads no offset index.

SET log_queries = 1;
SET use_parquet_metadata_cache = 1;
SET input_format_parquet_use_offset_index = 1;

-- One row group, several pages per column.
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
SELECT number AS a, number % 97 AS b, number AS z FROM numbers(100000)
SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100000, output_format_parquet_data_page_size = 65536;

-- The miss inserts the footer, the first hit keeps the offset indexes of the three columns, later reads use them.
SELECT sum(a), sum(b), sum(z) FROM s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:miss';
SELECT sum(a), sum(b), sum(z) FROM s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:hit_keeps';
SELECT sum(a), sum(b), sum(z) FROM s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:hit_kept';
SELECT count(), sum(z) FROM s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
WHERE b = 3 SETTINGS log_comment = '05512:filter';
-- The column index of `a` selects pages through its kept offset index.
SELECT count(), sum(z) FROM s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
WHERE a BETWEEN 30000 AND 30999 SETTINGS input_format_parquet_page_filter_push_down = 1, log_comment = '05512:page_filter';

-- Rewritten in place with two row groups: a new version, whose offset indexes are read and kept again.
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
SELECT number AS a, number % 97 AS b, number AS z FROM numbers(100000)
SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 50000, output_format_parquet_data_page_size = 65536;

SELECT sum(a), sum(b), sum(z) FROM s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:rewritten_miss';
SELECT sum(a), sum(b), sum(z) FROM s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:rewritten_hit_keeps';
SELECT sum(a), sum(b), sum(z) FROM s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:rewritten_hit_kept';

-- An offset index of about 1500 pages is larger than what the cache keeps for this small footer: it is never kept.
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_05512_pages.parquet', format = 'Parquet')
SELECT number AS a FROM numbers(200000)
SETTINGS s3_truncate_on_insert = 1, output_format_parquet_data_page_size = 1024, output_format_parquet_batch_size = 64;

SELECT sum(a) FROM s3(s3_conn, filename = currentDatabase() || '_05512_pages.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:large_miss';
SELECT sum(a) FROM s3(s3_conn, filename = currentDatabase() || '_05512_pages.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:large_hit_keeps';
SELECT sum(a) FROM s3(s3_conn, filename = currentDatabase() || '_05512_pages.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:large_hit_kept';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['ParquetMetadataCacheHits'] AS hits,
    ProfileEvents['ParquetMetadataCacheMisses'] AS misses,
    ProfileEvents['ParquetMetadataCacheOffsetIndexHits'] AS offset_index_hits
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment LIKE '05512:%'
ORDER BY event_time_microseconds;
