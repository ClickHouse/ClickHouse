-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: Iceberg needs Avro for its metadata
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- An insert into an Iceberg table with a text data file format used `ParallelFormattingOutputFormat`, whose thread
-- writes to the file while the insert reads the file size to decide where to cut it (a data race under TSan).

SET allow_insert_into_iceberg = 1, allow_experimental_iceberg_compaction = 0, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;
SET output_format_parallel_formatting = 1, max_threads = 4, max_insert_threads = 1, max_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0;
SET iceberg_insert_max_bytes_in_data_file = 100000;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05632_iceberg_insert_text_format_file_cut/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t (k Int64, s String) ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'JSONEachRow');
INSERT INTO t SELECT number, repeat('x', number % 100) FROM numbers(50000);

SELECT count(), sum(k), sum(length(s)) FROM t;
-- The data was cut into several files.
SELECT uniqExact(_path) > 1 FROM t;
