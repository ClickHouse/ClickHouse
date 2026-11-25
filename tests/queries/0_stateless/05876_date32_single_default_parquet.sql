-- Tags: no-fasttest
-- no-fasttest: Parquet is not available in fasttest builds
-- A Parquet `NULL` read as `Date32` under `input_format_null_as_default` is the type default, 1900-01-01,
-- which is also the value the pruning by row group statistics, page index and bloom filter assumes for it.

SET engine_file_truncate_on_insert = 1;
SET input_format_null_as_default = 1;
SET allow_suspicious_low_cardinality_types = 1;

-- One row group of 4 `NULL`s, one of 4 dates.
INSERT INTO FUNCTION file(currentDatabase() || '_05876.parquet', Parquet, 'd Nullable(Date32)')
SELECT if(number < 4, NULL, toDate32('2000-01-01') + number) FROM numbers(8)
SETTINGS output_format_parquet_row_group_size = 4;

SELECT 'values', arraySort(groupArray(d)) FROM file(currentDatabase() || '_05876.parquet', Parquet, 'd Date32');

SELECT 'd = 1900-01-01, push-down on', count() FROM file(currentDatabase() || '_05876.parquet', Parquet, 'd Date32')
WHERE d = '1900-01-01'
SETTINGS input_format_parquet_filter_push_down = 1, input_format_parquet_bloom_filter_push_down = 1,
    input_format_parquet_page_filter_push_down = 1, input_format_parquet_dictionary_filter_push_down = 1048576;
SELECT 'd = 1900-01-01, push-down off', count() FROM file(currentDatabase() || '_05876.parquet', Parquet, 'd Date32')
WHERE d = '1900-01-01'
SETTINGS input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
    input_format_parquet_page_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;
SELECT 'd = 1970-01-01, push-down on', count() FROM file(currentDatabase() || '_05876.parquet', Parquet, 'd Date32')
WHERE d = '1970-01-01'
SETTINGS input_format_parquet_filter_push_down = 1, input_format_parquet_bloom_filter_push_down = 1,
    input_format_parquet_page_filter_push_down = 1, input_format_parquet_dictionary_filter_push_down = 1048576;
SELECT 'd = 1970-01-01, push-down off', count() FROM file(currentDatabase() || '_05876.parquet', Parquet, 'd Date32')
WHERE d = '1970-01-01'
SETTINGS input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
    input_format_parquet_page_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;

-- Read as `LowCardinality(Date32)`, the decoded column is only wrapped, so a `NULL` keeps the type default.
SELECT 'LowCardinality, values', arraySort(groupArray(d)) FROM file(currentDatabase() || '_05876.parquet', Parquet, 'd LowCardinality(Date32)');

SELECT 'LowCardinality, d = 1900-01-01, push-down on', count() FROM file(currentDatabase() || '_05876.parquet', Parquet, 'd LowCardinality(Date32)')
WHERE d = '1900-01-01'
SETTINGS input_format_parquet_filter_push_down = 1, input_format_parquet_bloom_filter_push_down = 1,
    input_format_parquet_page_filter_push_down = 1, input_format_parquet_dictionary_filter_push_down = 1048576;
SELECT 'LowCardinality, d = 1900-01-01, push-down off', count() FROM file(currentDatabase() || '_05876.parquet', Parquet, 'd LowCardinality(Date32)')
WHERE d = '1900-01-01'
SETTINGS input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
    input_format_parquet_page_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;
SELECT 'LowCardinality, d = 1970-01-01, push-down on', count() FROM file(currentDatabase() || '_05876.parquet', Parquet, 'd LowCardinality(Date32)')
WHERE d = '1970-01-01'
SETTINGS input_format_parquet_filter_push_down = 1, input_format_parquet_bloom_filter_push_down = 1,
    input_format_parquet_page_filter_push_down = 1, input_format_parquet_dictionary_filter_push_down = 1048576;
SELECT 'LowCardinality, d = 1970-01-01, push-down off', count() FROM file(currentDatabase() || '_05876.parquet', Parquet, 'd LowCardinality(Date32)')
WHERE d = '1970-01-01'
SETTINGS input_format_parquet_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0,
    input_format_parquet_page_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;

-- Read as `Date`, the column is cast after decoding, and a `NULL` gets the default of `Date`.
SELECT 'read as Date', arraySort(groupArray(d)) FROM file(currentDatabase() || '_05876.parquet', Parquet, 'd Date');

INSERT INTO FUNCTION file(currentDatabase() || '_05876_array.parquet', Parquet, 'a Array(Nullable(Date32))')
SELECT [toDate32('2000-01-01'), NULL];
SELECT 'array element', a FROM file(currentDatabase() || '_05876_array.parquet', Parquet, 'a Array(Date32)');
SELECT 'array element, LowCardinality', a FROM file(currentDatabase() || '_05876_array.parquet', Parquet, 'a Array(LowCardinality(Date32))');
