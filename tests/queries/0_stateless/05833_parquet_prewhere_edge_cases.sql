-- PREWHERE edge cases of the Parquet reader, https://github.com/ClickHouse/ClickHouse/issues/132584

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05833.parquet')
    SELECT number AS x, toString(number) AS s, (number, toString(number * 10))::Tuple(a UInt64, b String) AS t
    FROM numbers(100) SETTINGS output_format_parquet_row_group_size = 10;

-- The writer cuts row groups along the input chunks, so `max_block_size` is pinned too.
INSERT INTO FUNCTION file(currentDatabase() || '_05833_pruning.parquet')
    SELECT number AS k, number * 2 AS e, toString(number) AS s FROM numbers(10000)
    SETTINGS max_block_size = 1000, output_format_parquet_row_group_size = 1000, output_format_parquet_data_page_size = 1024,
        output_format_parquet_batch_size = 100, output_format_parquet_write_page_index = 1,
        output_format_parquet_write_bloom_filter = 1;

-- { echoOn }

-- An expression in both PREWHERE and the select list.
SELECT x % 7 = 0 AS c, x FROM file(currentDatabase() || '_05833.parquet') PREWHERE x % 7 = 0 ORDER BY x LIMIT 4;
SELECT x + 1 AS y, s FROM file(currentDatabase() || '_05833.parquet') PREWHERE x + 1 > 97 ORDER BY x;
SELECT x % 7 = 0 FROM file(currentDatabase() || '_05833.parquet') PREWHERE x % 7 = 0 ORDER BY x LIMIT 3;
SELECT (x % 7 = 0) AND (s LIKE '%1%') AS c, x FROM file(currentDatabase() || '_05833.parquet') PREWHERE (x % 7 = 0) AND (s LIKE '%1%') ORDER BY x;

-- A Tuple column: file() does not support a subcolumn in PREWHERE, the whole tuple is fine.
SELECT t.a FROM file(currentDatabase() || '_05833.parquet') PREWHERE t.a = 5; -- { serverError ILLEGAL_PREWHERE }
SELECT t.a FROM file(currentDatabase() || '_05833.parquet') PREWHERE tupleElement(t, 'b') = '90' SETTINGS optimize_functions_to_subcolumns = 1; -- { serverError ILLEGAL_PREWHERE }
SELECT t.b FROM file(currentDatabase() || '_05833.parquet') PREWHERE t = tuple(5, '50');
SELECT t.b, x FROM file(currentDatabase() || '_05833.parquet') WHERE t.a = 5;
SELECT t.b FROM file(currentDatabase() || '_05833.parquet') PREWHERE x > 90 WHERE t.a % 2 = 0 ORDER BY x;

-- A PREWHERE that uses no columns.
SELECT count(), sum(x) FROM file(currentDatabase() || '_05833.parquet') PREWHERE rand() % 1 = 0;
SELECT count(), sum(x) FROM file(currentDatabase() || '_05833.parquet') PREWHERE rand() % 1 = 1;
SELECT s FROM file(currentDatabase() || '_05833.parquet') PREWHERE rand() % 1 = 0 AND x < 2 ORDER BY s;
SELECT count() FROM file(currentDatabase() || '_05833.parquet') PREWHERE rand() % 1 = 0 WHERE x < 5;
SELECT count() BETWEEN 1 AND 99 FROM file(currentDatabase() || '_05833.parquet') PREWHERE rand() % 2 = 0;

-- A constant PREWHERE.
SELECT count(), sum(x) FROM file(currentDatabase() || '_05833.parquet') PREWHERE 1;
SELECT count(), sum(x) FROM file(currentDatabase() || '_05833.parquet') PREWHERE 0;
SELECT count(), sum(x) FROM file(currentDatabase() || '_05833.parquet') PREWHERE toUInt8(1);
SELECT count(), sum(x) FROM file(currentDatabase() || '_05833.parquet') PREWHERE NULL;
SELECT count(), sum(x) FROM file(currentDatabase() || '_05833.parquet') PREWHERE toNullable(1);
SELECT count(), sum(x) FROM file(currentDatabase() || '_05833.parquet') PREWHERE toNullable(0);
SELECT count(), sum(x) FROM file(currentDatabase() || '_05833.parquet') PREWHERE toLowCardinality(toUInt8(0));
SELECT count(), sum(x) FROM file(currentDatabase() || '_05833.parquet') PREWHERE toLowCardinality(toNullable(toUInt8(1)));
SELECT x FROM file(currentDatabase() || '_05833.parquet') PREWHERE 1 WHERE x = 3;
SELECT count(), sum(x) FROM file(currentDatabase() || '_05833.parquet') PREWHERE toUInt8(1) WHERE x < 10;
SELECT s FROM file(currentDatabase() || '_05833.parquet') PREWHERE 0;

-- PREWHERE with IN (subquery).
SELECT x FROM file(currentDatabase() || '_05833.parquet') PREWHERE x IN (SELECT number * 11 FROM numbers(5)) ORDER BY x;
SELECT s FROM file(currentDatabase() || '_05833.parquet') PREWHERE x IN (SELECT x FROM file(currentDatabase() || '_05833.parquet') WHERE x > 97) ORDER BY s;
SELECT count(), sum(x) FROM file(currentDatabase() || '_05833.parquet') PREWHERE x NOT IN (SELECT number FROM numbers(90));
SELECT x FROM file(currentDatabase() || '_05833.parquet') PREWHERE (x, s) IN (SELECT number, toString(number) FROM numbers(3)) ORDER BY x;
SELECT count(), sum(x) FROM file(currentDatabase() || '_05833.parquet') PREWHERE x IN (SELECT number FROM numbers(0));

-- A column with a DEFAULT that is missing from the file, with the reader's filters on. file() does not support it in PREWHERE.
SET input_format_parquet_filter_push_down = 1, input_format_parquet_bloom_filter_push_down = 1, input_format_parquet_page_filter_push_down = 1;
SELECT count() FROM file(currentDatabase() || '_05833.parquet', Parquet, 'x UInt64, d UInt64 DEFAULT 42') WHERE d = 42;
SELECT count() FROM file(currentDatabase() || '_05833.parquet', Parquet, 'x UInt64, d UInt64 DEFAULT 42') WHERE d = 7;
SELECT x, d FROM file(currentDatabase() || '_05833.parquet', Parquet, 'x UInt64, d UInt64 DEFAULT x * 2') WHERE d = 10;
SELECT count(), min(x) FROM file(currentDatabase() || '_05833.parquet', Parquet, 'x UInt64, d UInt64 DEFAULT x * 2') WHERE d > 150;
SELECT x, d FROM file(currentDatabase() || '_05833.parquet', Parquet, 'x UInt64, d UInt64 DEFAULT x * 2') WHERE d IN (10, 20) ORDER BY x;
SELECT x, d FROM file(currentDatabase() || '_05833.parquet', Parquet, 'x UInt64, d UInt64 DEFAULT x * 2') PREWHERE x > 50 WHERE d = 120;
SELECT x, d FROM file(currentDatabase() || '_05833.parquet', Parquet, 'x UInt64, d UInt64 DEFAULT x * 2') PREWHERE d = 10; -- { serverError ILLEGAL_PREWHERE }

-- file() does not support a virtual column in PREWHERE, only in WHERE.
SELECT count() FROM file(currentDatabase() || '_05833.parquet') PREWHERE _file = currentDatabase() || '_05833.parquet'; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM file(currentDatabase() || '_05833.parquet') PREWHERE _row_number < 5; -- { serverError ILLEGAL_PREWHERE }
SELECT count(), sum(x) FROM file(currentDatabase() || '_05833.parquet') WHERE _row_number < 5;
SELECT count(), sum(x) FROM file(currentDatabase() || '_05833.parquet') WHERE _file = currentDatabase() || '_05833.parquet' AND x < 5;
SELECT count(), sum(x) FROM file(currentDatabase() || '_05833.parquet') PREWHERE x < 5 WHERE _row_number < 3;

-- Row group, bloom filter and page index pruning on columns used only in PREWHERE. 10 row groups of 1000 rows;
-- `e = 4321` is inside the min/max of one row group but absent from it. Each filter prunes nothing when it is off.
SET use_query_condition_cache = 0, use_parquet_metadata_cache = 0, input_format_parquet_dictionary_filter_push_down = 0, input_format_parquet_use_offset_index = 1;
SELECT s FROM file(currentDatabase() || '_05833_pruning.parquet') PREWHERE k = 4321 SETTINGS log_comment = 'k_point';
SELECT s FROM file(currentDatabase() || '_05833_pruning.parquet') PREWHERE k = 4321 SETTINGS log_comment = 'k_point_no_page_filter', input_format_parquet_page_filter_push_down = 0;
SELECT count(), min(s) FROM file(currentDatabase() || '_05833_pruning.parquet') PREWHERE k BETWEEN 4321 AND 4330 SETTINGS log_comment = 'k_range';
SELECT count(), any(s) FROM file(currentDatabase() || '_05833_pruning.parquet') PREWHERE e = 4321 SETTINGS log_comment = 'e_absent';
SELECT count(), any(s) FROM file(currentDatabase() || '_05833_pruning.parquet') PREWHERE e = 4321 SETTINGS log_comment = 'e_absent_no_bloom_filter', input_format_parquet_bloom_filter_push_down = 0;
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ParquetPrunedRowGroups'] AS pruned_by_stats,
    ProfileEvents['ParquetBloomFilterPrunedRowGroups'] AS pruned_by_bloom_filter, ProfileEvents['ParquetPrunedPages'] > 0 AS pruned_pages
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('k_point', 'k_point_no_page_filter', 'k_range', 'e_absent', 'e_absent_no_bloom_filter')
ORDER BY log_comment;
