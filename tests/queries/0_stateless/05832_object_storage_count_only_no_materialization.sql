-- Tags: no-fasttest
-- no-fasttest: Parquet and MinIO

SET optimize_count_from_files = 1, optimize_trivial_count_query = 1, use_cache_for_count_from_files = 0, parallel_replicas_for_cluster_engines = 0;

-- 10M rows of UInt64: the count chunk of the file holds all of them, 76 MiB if its placeholder were materialized.
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_count_big.parquet', format = Parquet, structure = 'x UInt64')
    SELECT 0 FROM numbers(10000000) SETTINGS s3_truncate_on_insert = 1;
SELECT count() FROM s3(s3_conn, filename = currentDatabase() || '_count_big.parquet', format = Parquet, structure = 'x UInt64')
    SETTINGS max_memory_usage = '40Mi';
