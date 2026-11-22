-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Tag no-fasttest: Iceberg needs Avro for its metadata
-- Tag no-parallel-replicas: profile events are not available on the second replica
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- A Parquet file of an object storage table whose footer is in the metadata cache and whose requested columns
-- are in the columns cache is read without opening it. Any other read opens the file as before.

SET allow_insert_into_iceberg = 1, allow_experimental_iceberg_compaction = 0, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;
SET use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1, use_parquet_metadata_cache = 1;
SET optimize_move_to_prewhere = 0, input_format_parquet_filter_push_down = 1;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05857_parquet_read_from_caches_only/') AS path
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

-- Not every requested column is in the columns cache.
SELECT 'other column', sum(n) FROM t SETTINGS log_comment = '05857_other_column';
-- PREWHERE is applied by the reader.
SELECT 'prewhere', count(), sum(id) FROM t PREWHERE id >= 4000 SETTINGS log_comment = '05857_prewhere';
-- The rows need their numbers in the file.
SELECT 'row number', sum(_row_number) FROM t SETTINGS log_comment = '05857_row_number';
-- The deleted rows are removed after the reader by their numbers.
DELETE FROM t WHERE id % 2 = 0;
SELECT 'deletes', count(), sum(id) FROM t SETTINGS log_comment = '05857_deletes';

SYSTEM FLUSH LOGS query_log;

-- The file is opened only if a cache misses (the columns cache may not admit or keep the columns of the first read).
SELECT log_comment,
    ProfileEvents['ObjectStorageReadObjects'] = 0 OR ProfileEvents['ColumnsCacheMisses'] + ProfileEvents['ParquetMetadataCacheMisses'] > 0,
    ProfileEvents['ParquetReadRowGroups']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05857_hot', '05857_where', '05857_virtual')
ORDER BY event_time_microseconds;

SELECT log_comment, ProfileEvents['ObjectStorageReadObjects'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05857_warm', '05857_other_column', '05857_prewhere', '05857_row_number', '05857_deletes')
ORDER BY event_time_microseconds;

DROP TABLE t;
DROP TABLE iceberg_path;
