-- Tags: no-fasttest
-- Tag no-fasttest: depends on S3
-- Random settings limits: output_format_parquet_row_group_size_bytes=(536870912, None); output_format_parquet_max_dictionary_size=(1048576, None)

-- A Parquet file in object storage whose ranges to read are all in the filesystem cache is read like a local file:
-- gaps between the needed column chunks are skipped (input_format_parquet_local_file_min_bytes_for_seek) instead of
-- being read (remote_read_min_bytes_for_seek), and the footer is read with a 64 KiB tail instead of the adaptive one.
-- If any range is not cached, the file is read as before. The file names are unique per database, so the cache starts
-- empty.

-- The reads go through the distributed cache by default where it is enabled, but the test checks the reads of this server.
SET table_engine_read_through_distributed_cache = 0;

-- One row group of three uncompressed column chunks: a (~1 MB), b (~2.3 MB), c (~1 MB). The settings limits keep this layout.
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_05836_f1.parquet', format = Parquet)
SELECT number AS a, toString(cityHash64(number)) AS b, number * 3 AS c FROM numbers(100000)
SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000000, output_format_parquet_compression_method = 'none',
    output_format_parquet_write_page_index = 0, output_format_parquet_write_bloom_filter = 0;
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_05836_f2.parquet', format = Parquet)
SELECT number AS a, toString(cityHash64(number)) AS b, number * 3 AS c FROM numbers(100000)
SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000000, output_format_parquet_compression_method = 'none',
    output_format_parquet_write_page_index = 0, output_format_parquet_write_bloom_filter = 0;
-- a (128 KiB), b (64 KiB) and c (128 KiB) are in the first 1 MiB file segment of the cache below.
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_05836_f3.parquet', format = Parquet)
SELECT number AS a, toUInt32(number) AS b, number * 3 AS c, repeat('x', 80) || toString(number) AS d FROM numbers(16384)
SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000000, output_format_parquet_compression_method = 'none',
    output_format_parquet_write_page_index = 0, output_format_parquet_write_bloom_filter = 0;

-- Without background download, a read of a and c leaves the segment partially downloaded.
CREATE TABLE cache_holder (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(type = cache, name = '05836_cache_no_background_download', path = '05836_cache_no_background_download/',
    max_size = '1Gi', max_file_segment_size = '1Mi', boundary_alignment = '1Mi', background_download_threads = 0, disk = 's3_disk');

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
-- A passive read (read_from_filesystem_cache_if_exists_otherwise_bypass_cache) reads a partially downloaded segment from S3, so
-- it keeps the gap between a and c to read both with one request. Other reads use the downloaded prefix.
SELECT sum(a), sum(c) FROM s3(s3_conn, filename = currentDatabase() || '_05836_f3.parquet', format = Parquet, structure = 'a UInt64, b UInt32, c UInt64, d String')
SETTINGS filesystem_cache_name = '05836_cache_no_background_download', log_comment = 'partial_read_1_a_c_cold';
SELECT sum(a), sum(c) FROM s3(s3_conn, filename = currentDatabase() || '_05836_f3.parquet', format = Parquet, structure = 'a UInt64, b UInt32, c UInt64, d String')
SETTINGS filesystem_cache_name = '05836_cache_no_background_download', read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 1,
    log_comment = 'partial_read_2_a_c_passive';
SELECT sum(a), sum(c) FROM s3(s3_conn, filename = currentDatabase() || '_05836_f3.parquet', format = Parquet, structure = 'a UInt64, b UInt32, c UInt64, d String')
SETTINGS filesystem_cache_name = '05836_cache_no_background_download', log_comment = 'partial_read_3_a_c';

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

WITH ProfileEvents['ParquetPrefetcherReadBytes'] - ProfileEvents['ParquetPrefetcherUsedBytes'] AS unused
SELECT replaceOne(log_comment, 'partial_read_', '') AS name, if(unused = 0, 'gaps not read', 'gap read') AS read,
    ProfileEvents['S3GetObject'] AS s3_requests
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND startsWith(log_comment, 'partial_read_')
ORDER BY name;
