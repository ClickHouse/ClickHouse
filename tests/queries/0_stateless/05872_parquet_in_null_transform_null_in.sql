-- Tags: no-fasttest
-- Parquet is not built in fasttest.
-- https://github.com/ClickHouse/ClickHouse/issues/133388
-- With `transform_null_in = 1`, `IN` with NULL must keep the NULL rows of Parquet row groups and pages
-- that min/max statistics could otherwise skip.

SET engine_file_truncate_on_insert = 1;
SET use_query_condition_cache = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_05872_null_in.parquet', Parquet, 'k Nullable(Int64)')
SELECT if(number < 10, NULL, number) FROM numbers(20) ORDER BY number
SETTINGS output_format_parquet_row_group_size = 10, output_format_parquet_write_page_index = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05872_null_in_partial.parquet', Parquet, 'k Nullable(Int64)')
SELECT x FROM (SELECT arrayJoin([NULL, 60, 70, 500, 501, 502]) AS x) ORDER BY x NULLS FIRST
SETTINGS output_format_parquet_row_group_size = 3, output_format_parquet_write_page_index = 1;

SELECT 'row group statistics';
SET input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 0,
    input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;
SELECT count() FROM file(currentDatabase() || '_05872_null_in.parquet', Parquet, 'k Nullable(Int64)') WHERE k IN (NULL, 15) SETTINGS transform_null_in = 1;
SELECT count() FROM file(currentDatabase() || '_05872_null_in_partial.parquet', Parquet, 'k Nullable(Int64)') WHERE k IN (NULL, 501) SETTINGS transform_null_in = 1;
SELECT count() FROM file(currentDatabase() || '_05872_null_in.parquet', Parquet, 'k Nullable(Int64)') WHERE k NOT IN (NULL, 15) SETTINGS transform_null_in = 1;
SELECT count() FROM file(currentDatabase() || '_05872_null_in.parquet', Parquet, 'k Nullable(Int64)') WHERE k IN (NULL, 15) SETTINGS transform_null_in = 0;
SELECT count() FROM file(currentDatabase() || '_05872_null_in.parquet', Parquet, 'k Nullable(Int64)') WHERE k IS NULL;
SELECT count() FROM file(currentDatabase() || '_05872_null_in.parquet', Parquet, 'k Nullable(Int64)') WHERE k IS NOT NULL;
SELECT count() FROM file(currentDatabase() || '_05872_null_in.parquet', Parquet, 'k Nullable(Int64)') WHERE k < 15;
SELECT count() FROM file(currentDatabase() || '_05872_null_in_partial.parquet', Parquet, 'k Nullable(Int64)') WHERE k > 65;

SELECT 'page index';
SET input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 1;
SELECT count() FROM file(currentDatabase() || '_05872_null_in.parquet', Parquet, 'k Nullable(Int64)') WHERE k IN (NULL, 15) SETTINGS transform_null_in = 1;
SELECT count() FROM file(currentDatabase() || '_05872_null_in_partial.parquet', Parquet, 'k Nullable(Int64)') WHERE k IN (NULL, 501) SETTINGS transform_null_in = 1;
SELECT count() FROM file(currentDatabase() || '_05872_null_in.parquet', Parquet, 'k Nullable(Int64)') WHERE k NOT IN (NULL, 15) SETTINGS transform_null_in = 1;
SELECT count() FROM file(currentDatabase() || '_05872_null_in.parquet', Parquet, 'k Nullable(Int64)') WHERE k IS NULL;
SELECT count() FROM file(currentDatabase() || '_05872_null_in.parquet', Parquet, 'k Nullable(Int64)') WHERE k IS NOT NULL;
