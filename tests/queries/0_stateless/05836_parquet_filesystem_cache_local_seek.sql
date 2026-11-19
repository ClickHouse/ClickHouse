-- Tags: no-fasttest
-- Tag no-fasttest: depends on S3

-- A Parquet file in object storage whose ranges to read are all in the filesystem cache is read like a local file:
-- gaps between the needed column chunks are skipped (input_format_parquet_local_file_min_bytes_for_seek) instead of
-- being read (remote_read_min_bytes_for_seek), and the footer is read with a 64 KiB tail instead of the adaptive one.
-- If any range is not cached, the file is read as before. The file names are unique per database, so the cache starts
-- empty.

-- One row group of three uncompressed column chunks: a (~1 MB), b (~2.3 MB), c (~1 MB).
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_05836_f1.parquet', format = Parquet)
SELECT number AS a, toString(cityHash64(number)) AS b, number * 3 AS c FROM numbers(100000)
SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000000, output_format_parquet_compression_method = 'none',
    output_format_parquet_write_page_index = 0, output_format_parquet_write_bloom_filter = 0;
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_05836_f2.parquet', format = Parquet)
SELECT number AS a, toString(cityHash64(number)) AS b, number * 3 AS c FROM numbers(100000)
SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000000, output_format_parquet_compression_method = 'none',
    output_format_parquet_write_page_index = 0, output_format_parquet_write_bloom_filter = 0;

SET filesystem_cache_name = 'cache_for_readbigat', enable_filesystem_cache = 1, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0,
    remote_read_min_bytes_for_seek = 4194304, input_format_parquet_local_file_min_bytes_for_seek = 8192, input_format_parquet_footer_read_size = 0,
    remote_filesystem_read_prefetch = 0, use_page_cache_for_object_storage = 0, use_columns_cache = 0, use_parquet_metadata_cache = 0,
    enable_parallel_replicas = 0, optimize_count_from_files = 1, optimize_trivial_count_query = 1, use_cache_for_count_from_files = 0;

SELECT count() FROM s3(s3_conn, filename = currentDatabase() || '_05836_f1.parquet', format = Parquet, structure = 'a UInt64, b String, c UInt64')
SETTINGS log_comment = 'cached_read_1_count_cold';
SELECT count() FROM s3(s3_conn, filename = currentDatabase() || '_05836_f1.parquet', format = Parquet, structure = 'a UInt64, b String, c UInt64')
SETTINGS log_comment = 'cached_read_2_count_cached';
SELECT sum(a), sum(c) FROM s3(s3_conn, filename = currentDatabase() || '_05836_f1.parquet', format = Parquet, structure = 'a UInt64, b String, c UInt64')
SETTINGS log_comment = 'cached_read_3_a_c_cold';
SELECT sum(a), sum(c) FROM s3(s3_conn, filename = currentDatabase() || '_05836_f1.parquet', format = Parquet, structure = 'a UInt64, b String, c UInt64')
SETTINGS log_comment = 'cached_read_4_a_c_cached';
-- Only a and the footer of f2 are cached, so c is read from S3 with the gap before it.
SELECT sum(a) FROM s3(s3_conn, filename = currentDatabase() || '_05836_f2.parquet', format = Parquet, structure = 'a UInt64, b String, c UInt64')
SETTINGS log_comment = 'cached_read_5_a_cold';
SELECT sum(a), sum(c) FROM s3(s3_conn, filename = currentDatabase() || '_05836_f2.parquet', format = Parquet, structure = 'a UInt64, b String, c UInt64')
SETTINGS log_comment = 'cached_read_6_a_c_partly_cached';

SYSTEM FLUSH LOGS query_log;

WITH ProfileEvents['ParquetPrefetcherReadBytes'] - ProfileEvents['ParquetPrefetcherUsedBytes'] AS unused
SELECT
    replaceOne(log_comment, 'cached_read_', '') AS name,
    if(name LIKE '%count%', toString(ProfileEvents['ParquetPrefetcherReadBytes']),
        multiIf(unused < 100000, 'gaps not read', unused > 1000000, 'gap read', toString(unused))) AS read
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND startsWith(log_comment, 'cached_read_')
ORDER BY name;
