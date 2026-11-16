-- Tags: no-fasttest
-- no-fasttest: Parquet support is not built in fast-test images

-- Splitting a single Parquet file between several sources respects `use_parquet_metadata_cache`.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05441.parquet')
SELECT number AS id FROM numbers(8000)
SETTINGS output_format_parquet_row_group_size = 1000, output_format_parquet_use_custom_encoder = 1;

SELECT sum(id) FROM file(currentDatabase() || '_05441.parquet', Parquet, 'id UInt64')
SETTINGS max_threads = 4, parallelize_output_from_storages = 1, input_format_parquet_preserve_order = 0,
    use_parquet_metadata_cache = 0, log_comment = '05441_disabled';

SELECT sum(id) FROM file(currentDatabase() || '_05441.parquet', Parquet, 'id UInt64')
SETTINGS max_threads = 4, parallelize_output_from_storages = 1, input_format_parquet_preserve_order = 0,
    use_parquet_metadata_cache = 1, log_comment = '05441_enabled';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['ParquetMetadataCacheHits'] + ProfileEvents['ParquetMetadataCacheMisses'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05441_disabled', '05441_enabled')
ORDER BY log_comment;
