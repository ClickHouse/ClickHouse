-- Tags: no-fasttest
-- no-fasttest: the Parquet format is not built in the fast-test image.

-- Parquet min/max statistics stand a NULL in as -inf. A row group or page holding NULLs must be kept
-- for a predicate that maps NULL to a matching value: ifNull/coalesce/assumeNotNull chains and sets
-- holding NULL under transform_null_in.

SET engine_file_truncate_on_insert = 1;
-- Bloom and dictionary filters are not under test.
SET input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;

-- Every row group and page holds 1% NULLs and -1 otherwise.
INSERT INTO FUNCTION file(currentDatabase() || '_mixed.parquet', Parquet)
    SELECT if(number % 100 = 0, NULL, -1)::Nullable(Int64) AS id FROM numbers(20000)
    SETTINGS output_format_parquet_row_group_size = 10000, max_block_size = 1000;
-- Row group 0 is all NULL.
INSERT INTO FUNCTION file(currentDatabase() || '_all_null.parquet', Parquet)
    SELECT if(number < 10000, NULL, number)::Nullable(Int64) AS id FROM numbers(20000)
    SETTINGS output_format_parquet_row_group_size = 10000, max_block_size = 1000;

SELECT '-- row groups';
SET input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 0;
SELECT count() FROM file(currentDatabase() || '_mixed.parquet', Parquet, 'id Nullable(Int64)') WHERE toFloat64(ifNull(id, toInt64(42))) = 42;
SELECT count() FROM file(currentDatabase() || '_mixed.parquet', Parquet, 'id Nullable(Int64)') WHERE ifNull(id, toInt64(42)) IN (42, 43);
SELECT count() FROM file(currentDatabase() || '_mixed.parquet', Parquet, 'id Nullable(Int64)') WHERE toInt64(coalesce(id, toInt64(42))) = 42;
SELECT count() FROM file(currentDatabase() || '_mixed.parquet', Parquet, 'id Nullable(Int64)') WHERE assumeNotNull(id) = 0;
SELECT count() FROM file(currentDatabase() || '_mixed.parquet', Parquet, 'id Nullable(Int64)') WHERE intDiv(id, 10) IN (NULL, 5) SETTINGS transform_null_in = 1;
SELECT count() FROM file(currentDatabase() || '_mixed.parquet', Parquet, 'id Nullable(Int64)') WHERE id IN (NULL, 5) SETTINGS transform_null_in = 1;
SELECT count() FROM file(currentDatabase() || '_mixed.parquet', Parquet, 'id Nullable(Int64)') WHERE ifNull(id, toInt64(42)) = 42 SETTINGS allow_key_condition_coalesce_rewrite = 0;
SELECT count() FROM file(currentDatabase() || '_all_null.parquet', Parquet, 'id Nullable(Int64)') WHERE intDiv(id, 10) IN (NULL, 5) SETTINGS transform_null_in = 1;
SELECT count() FROM file(currentDatabase() || '_all_null.parquet', Parquet, 'id Nullable(Int64)') WHERE id IN (NULL, 5) SETTINGS transform_null_in = 1;

SELECT '-- pages';
SET input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 1;
SELECT count() FROM file(currentDatabase() || '_mixed.parquet', Parquet, 'id Nullable(Int64)') WHERE toFloat64(ifNull(id, toInt64(42))) = 42;
SELECT count() FROM file(currentDatabase() || '_mixed.parquet', Parquet, 'id Nullable(Int64)') WHERE ifNull(id, toInt64(42)) IN (42, 43);
SELECT count() FROM file(currentDatabase() || '_mixed.parquet', Parquet, 'id Nullable(Int64)') WHERE toInt64(coalesce(id, toInt64(42))) = 42;
SELECT count() FROM file(currentDatabase() || '_mixed.parquet', Parquet, 'id Nullable(Int64)') WHERE assumeNotNull(id) = 0;
SELECT count() FROM file(currentDatabase() || '_mixed.parquet', Parquet, 'id Nullable(Int64)') WHERE intDiv(id, 10) IN (NULL, 5) SETTINGS transform_null_in = 1;
SELECT count() FROM file(currentDatabase() || '_mixed.parquet', Parquet, 'id Nullable(Int64)') WHERE id IN (NULL, 5) SETTINGS transform_null_in = 1;
SELECT count() FROM file(currentDatabase() || '_all_null.parquet', Parquet, 'id Nullable(Int64)') WHERE intDiv(id, 10) IN (NULL, 5) SETTINGS transform_null_in = 1;
SELECT count() FROM file(currentDatabase() || '_all_null.parquet', Parquet, 'id Nullable(Int64)') WHERE id IN (NULL, 5) SETTINGS transform_null_in = 1;

SELECT '-- row groups holding NULLs are still pruned for an ordinary predicate';
SELECT count() FROM file(currentDatabase() || '_mixed.parquet', Parquet, 'id Nullable(Int64)') WHERE id > 0
    SETTINGS input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 0, log_comment = '05496_prune_check';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['ParquetPrunedRowGroups'] FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment = '05496_prune_check' AND type = 'QueryFinish';
