-- Tags: no-fasttest, no-parallel, no-parallel-replicas, no-replicated-database
-- Tag no-fasttest: Iceberg needs Avro for its metadata
-- Tag no-parallel: the columns cache is system-wide; the test drops it so that its reads are admitted, and asserts what they hit
-- Tag no-parallel-replicas: profile events are not available on the second replica
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- A Parquet file of an object storage table whose footer is in the metadata cache and whose requested columns
-- are in the columns cache is read without opening it. Any other read opens the file as before.

SET allow_insert_into_iceberg = 1, allow_experimental_iceberg_compaction = 0, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;
SET use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1, use_parquet_metadata_cache = 1;
SET optimize_move_to_prewhere = 0, input_format_parquet_filter_push_down = 1;
SYSTEM DROP COLUMNS CACHE;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path,
    concat(if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')), user_files_path, '/', currentDatabase()) AS prefix
SELECT concat(prefix, '/05857_parquet_read_from_caches_only/') AS path, concat(prefix, '/05857_parquet_read_from_caches_only_evolved/') AS path_evolved
FROM system.server_settings WHERE name = 'user_files_path';

-- One data file with 5 row groups.
CREATE TABLE t (id Int64, s String, n Int64) ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet')
SETTINGS output_format_parquet_row_group_size = 1000, output_format_parquet_row_group_size_bytes = 536870912;
INSERT INTO t SELECT number, toString(number), number * 2 FROM numbers(5000) SETTINGS max_threads = 1, max_insert_threads = 1;

SELECT 'warm', sum(id), sum(length(s)) FROM t SETTINGS log_comment = '05857_warm';
SELECT 'hot', sum(id), sum(length(s)) FROM t SETTINGS log_comment = '05857_hot';
SELECT 'where', count(), sum(id) FROM t WHERE id >= 4000 SETTINGS log_comment = '05857_where';
SELECT 'where uncached', count(), sum(id) FROM t WHERE id >= 4000 SETTINGS use_columns_cache = 0;
SELECT 'virtual', count(), countIf(_file != ''), sum(id) FROM t SETTINGS log_comment = '05857_virtual';

-- The same file in S3, read through the filesystem cache.
INSERT INTO FUNCTION s3(s3_conn, filename = concat(currentDatabase(), '_05857/data.parquet'), format = Parquet)
SELECT number AS id, toString(number) AS s FROM numbers(5000)
SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000, output_format_parquet_row_group_size_bytes = 536870912;
SELECT 's3 warm', sum(id), sum(length(s))
FROM s3(s3_conn, filename = concat(currentDatabase(), '_05857/data.parquet'), format = Parquet, structure = 'id Int64, s String')
SETTINGS filesystem_cache_name = 'cache_for_readbigat', enable_filesystem_cache = 1, remote_filesystem_read_method = 'threadpool',
    remote_filesystem_read_prefetch = 1, max_read_buffer_size = 1048576, log_comment = '05857_s3_warm';
SELECT 's3 hot', sum(id), sum(length(s))
FROM s3(s3_conn, filename = concat(currentDatabase(), '_05857/data.parquet'), format = Parquet, structure = 'id Int64, s String')
SETTINGS filesystem_cache_name = 'cache_for_readbigat', enable_filesystem_cache = 1, remote_filesystem_read_method = 'threadpool',
    remote_filesystem_read_prefetch = 1, max_read_buffer_size = 1048576, log_comment = '05857_s3_hot';

-- Not every requested column is in the columns cache.
SELECT 'other column', sum(n) FROM t SETTINGS log_comment = '05857_other_column';
-- PREWHERE is applied by the reader.
SELECT 'prewhere', count(), sum(id) FROM t PREWHERE id >= 4000 SETTINGS log_comment = '05857_prewhere';
-- The rows need their numbers in the file.
SELECT 'row number', sum(_row_number) FROM t SETTINGS log_comment = '05857_row_number';
-- The top-K threshold is applied by the reader.
SELECT 'top-k', id FROM t ORDER BY id DESC LIMIT 3
SETTINGS use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, log_comment = '05857_top_k';
SELECT 'top-k off', id FROM t ORDER BY id DESC LIMIT 3 SETTINGS use_top_k_dynamic_filtering = 0, log_comment = '05857_top_k_off';

-- The file has the old type of a column.
CREATE TABLE t_evolved (id Int32, s String) ENGINE = IcebergLocal((SELECT path_evolved FROM iceberg_path), 'Parquet');
INSERT INTO t_evolved SELECT number, toString(number) FROM numbers(1000);
SELECT 'before evolution', sum(id) FROM t_evolved;
ALTER TABLE t_evolved MODIFY COLUMN id Int64;
SELECT 'evolved', sum(id), toTypeName(any(id)) FROM t_evolved SETTINGS log_comment = '05857_evolved';
SELECT 'evolved', sum(id), toTypeName(any(id)) FROM t_evolved SETTINGS log_comment = '05857_evolved';

-- The deleted rows are removed after the reader by their numbers.
DELETE FROM t WHERE id % 2 = 0;
SELECT 'deletes', count(), sum(id) FROM t SETTINGS log_comment = '05857_deletes';

SYSTEM FLUSH LOGS query_log;

-- Read from the caches only: no object opened, no cache missed.
SELECT log_comment,
    ProfileEvents['ObjectStorageReadObjects'], ProfileEvents['ColumnsCacheMisses'], ProfileEvents['ParquetMetadataCacheMisses'],
    ProfileEvents['ColumnsCacheHits'], ProfileEvents['ParquetReadRowGroups']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05857_hot', '05857_where', '05857_virtual', '05857_top_k_off')
ORDER BY event_time_microseconds;

-- Without a read buffer, neither the whole-file prefetch of a small file nor a read from the filesystem cache.
SELECT log_comment,
    ProfileEvents['ObjectStorageReadObjects'], ProfileEvents['ColumnsCacheMisses'], ProfileEvents['ParquetMetadataCacheMisses'],
    ProfileEvents['ColumnsCacheHits'], ProfileEvents['ParquetReadRowGroups'],
    ProfileEvents['RemoteFSPrefetches'], ProfileEvents['CachedReadBufferReadFromCacheBytes']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05857_s3_hot';

SELECT log_comment, ProfileEvents['ObjectStorageReadObjects'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05857_warm', '05857_s3_warm', '05857_other_column', '05857_prewhere', '05857_row_number', '05857_top_k', '05857_evolved', '05857_deletes')
ORDER BY event_time_microseconds;

DROP TABLE t;
DROP TABLE t_evolved;
DROP TABLE iceberg_path;
