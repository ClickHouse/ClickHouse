-- Tags: no-fasttest
-- no-fasttest: Parquet support is not built in fast-test images
-- Random settings limits: use_columns_cache=(0, 0)
-- use_columns_cache: columns shared with the columns cache are copied, not filtered in place; 05338 tests that path.

-- PREWHERE over a Parquet file, with filters that keep most or exactly half of the rows of each block,
-- over columns that are read by the filter and also selected.

SET input_format_parquet_max_block_size = 1000;

INSERT INTO FUNCTION file(currentDatabase() || '_05497.parquet', Parquet)
SELECT
    number AS k,
    if(number % 10 = 0, '', concat('str_', toString(number))) AS s,
    if(number % 7 = 0, NULL, toString(number * 3)) AS ns,
    arrayMap(i -> toUInt8(i + number % 3), range(number % 5)) AS au8,
    arrayMap(i -> toString(number + i), range(number % 4)) AS astr,
    toFixedString(leftPad(toString(number % 10000), 4, '0'), 4) AS fs,
    toDecimal64(number / 7, 3) AS d,
    (number * 3, toString(number)) AS t,
    map(toString(number % 13), number) AS m
FROM numbers(5000)
SETTINGS output_format_parquet_row_group_size = 2500, engine_file_truncate_on_insert = 1;

SELECT 'most rows pass';
SELECT count(), sum(cityHash64(s)) FROM file(currentDatabase() || '_05497.parquet') PREWHERE cityHash64(s) % 10 != 0;
SELECT count(), sum(cityHash64(ns)) FROM file(currentDatabase() || '_05497.parquet') PREWHERE cityHash64(ns) % 10 != 0;
SELECT count(), sum(cityHash64(au8)) FROM file(currentDatabase() || '_05497.parquet') PREWHERE cityHash64(au8) % 10 != 0;
SELECT count(), sum(cityHash64(astr)) FROM file(currentDatabase() || '_05497.parquet') PREWHERE cityHash64(astr) % 10 != 0;
SELECT count(), sum(cityHash64(fs)) FROM file(currentDatabase() || '_05497.parquet') PREWHERE cityHash64(fs) % 10 != 0;
SELECT count(), sum(cityHash64(d)) FROM file(currentDatabase() || '_05497.parquet') PREWHERE cityHash64(d) % 10 != 0;
SELECT count(), sum(cityHash64(t)) FROM file(currentDatabase() || '_05497.parquet') PREWHERE cityHash64(t) % 10 != 0;
SELECT count(), sum(cityHash64(m)) FROM file(currentDatabase() || '_05497.parquet') PREWHERE cityHash64(m) % 10 != 0;

SELECT 'exactly half of the rows pass';
SELECT count(), sum(k), sum(cityHash64(s)) FROM file(currentDatabase() || '_05497.parquet') PREWHERE k % 2 = 0;

SELECT 'a tuple and its element';
-- A subcolumn is not supported in the PREWHERE of `file`, so the filter reads the whole tuple.
SELECT count(), sum(cityHash64(t)), sum(t.1) FROM file(currentDatabase() || '_05497.parquet') PREWHERE tupleElement(t, 1) % 4 != 0
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'two steps';
SELECT count(), sum(k), sum(cityHash64(s)) FROM file(currentDatabase() || '_05497.parquet') PREWHERE k % 10 != 0 AND s != ''
SETTINGS enable_multiple_prewhere_read_steps = 1, allow_reorder_prewhere_conditions = 0;

SELECT 'constant and nested columns in the filter';
INSERT INTO FUNCTION file(currentDatabase() || '_05497_v.parquet', Parquet, 'v Nullable(UInt8)')
SELECT arrayJoin([0, 1, 2, 3]) SETTINGS engine_file_truncate_on_insert = 1;
SELECT v FROM file(currentDatabase() || '_05497_v.parquet') PREWHERE and(materialize(255), *) ORDER BY v;
INSERT INTO FUNCTION file(currentDatabase() || '_05497_x.parquet', Parquet, 'x Int16')
SELECT arrayJoin([1, 2, 1, 3, 100]) SETTINGS engine_file_truncate_on_insert = 1;
SELECT median(3) IGNORE NULLS FROM file(currentDatabase() || '_05497_x.parquet')
PREWHERE and(materialize(toNullable(materialize(1))), not(materialize(100) = *));
