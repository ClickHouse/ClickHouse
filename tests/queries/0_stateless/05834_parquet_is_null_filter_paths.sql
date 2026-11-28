-- IS NULL and IS NOT NULL through each Parquet filter path: the page index, the bloom filter, the dictionary
-- filter and PREWHERE (the row group statistics alone: 05372). ProfileEvents show what each path pruned.

SET engine_file_truncate_on_insert = 1;
SET use_query_condition_cache = 0, use_parquet_metadata_cache = 0;

-- Row groups of 1000 rows and pages of 100 rows: all NULL; no NULL; 3 pages with some NULLs, 4 all-NULL pages
-- and 3 pages without NULLs; no NULL. x = 2 * id otherwise, so the odd 2001 is inside the second row group's
-- min/max and absent from it.
CREATE TABLE src (id UInt32, x Nullable(Int64)) ENGINE = Memory AS
SELECT number, if(number < 1000 OR number BETWEEN 2300 AND 2699 OR (number BETWEEN 2000 AND 2299 AND number % 10 = 0), NULL, 2 * number)
FROM numbers(4000) SETTINGS max_block_size = 65409;

INSERT INTO FUNCTION file(currentDatabase() || '_05834.parquet') SELECT * FROM src
SETTINGS output_format_parquet_row_group_size = 1000, output_format_parquet_row_group_size_bytes = 536870912,
    output_format_parquet_data_page_size = 1, output_format_parquet_batch_size = 100,
    output_format_parquet_write_page_index = 1, output_format_parquet_write_bloom_filter = 1,
    output_format_parquet_bloom_filter_bits_per_value = 10.5, output_format_parquet_max_dictionary_size = 1048576;

SELECT 'expected';
SELECT count(), sum(id) FROM src WHERE x IS NULL;
SELECT count(), sum(id) FROM src WHERE x IS NOT NULL;
SELECT id, x FROM src WHERE x IS NULL AND id % 500 = 0 ORDER BY id;
SELECT id, x FROM src WHERE x IS NOT NULL AND id % 500 = 0 ORDER BY id;

SELECT 'page index';
SET input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 1,
    input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 0;
SELECT count(), sum(id) FROM file(currentDatabase() || '_05834.parquet') WHERE x IS NULL SETTINGS log_comment = 'q05834_page_is_null';
SELECT count(), sum(id) FROM file(currentDatabase() || '_05834.parquet') WHERE x IS NOT NULL SETTINGS log_comment = 'q05834_page_is_not_null';
SELECT count(), sum(id) FROM file(currentDatabase() || '_05834.parquet') WHERE NOT (x IS NULL) SETTINGS log_comment = 'q05834_page_not_is_null';
SELECT count(), sum(id) FROM file(currentDatabase() || '_05834.parquet') WHERE x IS NULL
SETTINGS log_comment = 'q05834_page_off', input_format_parquet_page_filter_push_down = 0;

SELECT 'bloom filter';
-- Bloom and dictionary filters hold no NULLs: a bare IS NULL / IS NOT NULL prunes nothing by them, the OR form does on a row group without NULLs.
SET input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0,
    input_format_parquet_bloom_filter_push_down = 1, input_format_parquet_dictionary_filter_push_down = 0;
SELECT count(), sum(id) FROM file(currentDatabase() || '_05834.parquet') WHERE x IS NULL SETTINGS log_comment = 'q05834_bloom_is_null';
SELECT count(), sum(id) FROM file(currentDatabase() || '_05834.parquet') WHERE x IS NOT NULL SETTINGS log_comment = 'q05834_bloom_is_not_null';
SELECT count(), sum(id) FROM file(currentDatabase() || '_05834.parquet') WHERE x IS NULL OR x = 2001
SETTINGS log_comment = 'q05834_bloom_or', input_format_parquet_filter_push_down = 1;
SELECT count(), sum(id) FROM file(currentDatabase() || '_05834.parquet') WHERE x IS NULL OR x = 2001
SETTINGS log_comment = 'q05834_bloom_or_off', input_format_parquet_filter_push_down = 1, input_format_parquet_bloom_filter_push_down = 0;

SELECT 'dictionary filter';
-- The dictionary filter skips pruning when its memory budget is too small for the dictionary.
SET input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0,
    input_format_parquet_bloom_filter_push_down = 0, input_format_parquet_dictionary_filter_push_down = 1048576,
    input_format_parquet_memory_high_watermark = 4294967296;
SELECT count(), sum(id) FROM file(currentDatabase() || '_05834.parquet') WHERE x IS NULL SETTINGS log_comment = 'q05834_dict_is_null';
SELECT count(), sum(id) FROM file(currentDatabase() || '_05834.parquet') WHERE x IS NOT NULL SETTINGS log_comment = 'q05834_dict_is_not_null';
SELECT count(), sum(id) FROM file(currentDatabase() || '_05834.parquet') WHERE x IS NULL OR x = 2001
SETTINGS log_comment = 'q05834_dict_or', input_format_parquet_filter_push_down = 1;
SELECT count(), sum(id) FROM file(currentDatabase() || '_05834.parquet') WHERE x IS NULL OR x = 2001
SETTINGS log_comment = 'q05834_dict_or_off', input_format_parquet_filter_push_down = 1, input_format_parquet_dictionary_filter_push_down = 0;

SELECT 'PREWHERE';
SET input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 1,
    input_format_parquet_bloom_filter_push_down = 1, input_format_parquet_dictionary_filter_push_down = 1048576;
SELECT count(), sum(id) FROM file(currentDatabase() || '_05834.parquet') PREWHERE x IS NULL SETTINGS log_comment = 'q05834_prewhere_is_null';
SELECT count(), sum(id) FROM file(currentDatabase() || '_05834.parquet') PREWHERE x IS NOT NULL SETTINGS log_comment = 'q05834_prewhere_is_not_null';
SELECT count(), sum(id) FROM file(currentDatabase() || '_05834.parquet') PREWHERE x IS NULL OR x = 2001 SETTINGS log_comment = 'q05834_prewhere_or';
SELECT id, x FROM file(currentDatabase() || '_05834.parquet') PREWHERE isNull(x) AND id % 500 = 0 ORDER BY id;
SELECT id, x FROM file(currentDatabase() || '_05834.parquet') PREWHERE isNotNull(x) AND id % 500 = 0 ORDER BY id;
SELECT count(), sum(id) FROM file(currentDatabase() || '_05834.parquet') PREWHERE x IS NULL
SETTINGS log_comment = 'q05834_prewhere_off', input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;

SYSTEM FLUSH LOGS query_log;
SELECT replaceOne(log_comment, 'q05834_', ''), ProfileEvents['ParquetPrunedRowGroups'], ProfileEvents['ParquetBloomFilterPrunedRowGroups'],
    ProfileEvents['ParquetDictionaryFilterPrunedRowGroups'], ProfileEvents['ParquetPrunedPages']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'q05834\\_%'
ORDER BY event_time_microseconds;

-- The reader evaluates PREWHERE only on the rows of the row groups and pages left after pruning.
SELECT replaceOne(log_comment, 'q05834_', ''), ProfileEvents['ParquetRowsFilterExpression']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'q05834\\_prewhere\\_%'
ORDER BY event_time_microseconds;
