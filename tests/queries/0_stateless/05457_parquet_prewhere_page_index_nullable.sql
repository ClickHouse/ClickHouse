-- Tags: no-fasttest
-- Columns read after PREWHERE steps from Parquet files with and without a page index, compared with the source table.
-- Nullable columns with scattered NULLs and with all-NULL pages, PLAIN and dictionary-encoded pages, small pages that
-- straddle the 1000-row blocks; strings and arrays are read too.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS pq_idx;
DROP TABLE IF EXISTS pq_noidx;

CREATE TABLE src ENGINE = Memory AS SELECT
    toUInt32(number) AS k,
    toUInt8(number % 4) AS x,
    toInt8(cityHash64(number, 1)) AS c_i8,
    toInt32(cityHash64(number, 2)) AS c_i32,
    toInt64(cityHash64(number, 3)) AS c_i64,
    toFloat32(cityHash64(number, 4) % 1000000 / 7) AS c_f32,
    toDecimal64(toInt64(cityHash64(number, 5) % 1999999999999) - 999999999999, 5) AS c_d64,
    toDecimal128(toInt128(cityHash64(number, 6)) * 1000000, 10) AS c_d128,
    toFixedString(substring(hex(cityHash64(number, 7)), 1, 3), 3) AS c_fs3,
    reinterpretAsUUID(reinterpretAsFixedString(toUInt128(cityHash64(number, 8)) * cityHash64(number, 9))) AS c_uuid,
    toDecimal128(cityHash64(number, 10) % 50, 2) AS c_dict,
    if(cityHash64(number, 11) % 5 = 0, NULL, toInt32(cityHash64(number, 12))) AS n_i32,
    if(intDiv(number, 700) % 3 = 1, NULL, toDecimal128(toInt128(cityHash64(number, 13)), 10)) AS n_d128,
    hex(cityHash64(number, 14)) AS s,
    arrayMap(i -> toInt32(cityHash64(number, i)), range(number % 3)) AS arr
FROM numbers(5003);

-- A File table takes its format settings from its own SETTINGS clause, not from the query.
-- The small dictionary limit keeps only the low-cardinality columns dictionary-encoded.
CREATE TABLE pq_idx AS src ENGINE = File(Parquet) SETTINGS
    output_format_parquet_write_page_index = 1,
    output_format_parquet_max_dictionary_size = 1024,
    output_format_parquet_data_page_size = 1000,
    output_format_parquet_batch_size = 97,
    input_format_parquet_max_block_size = 1000;
INSERT INTO pq_idx SELECT * FROM src;

CREATE TABLE pq_noidx AS src ENGINE = File(Parquet) SETTINGS
    output_format_parquet_write_page_index = 0,
    output_format_parquet_max_dictionary_size = 1024,
    output_format_parquet_data_page_size = 1000,
    output_format_parquet_batch_size = 97,
    input_format_parquet_max_block_size = 1000;
INSERT INTO pq_noidx SELECT * FROM src;

SELECT 'every row';
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_idx PREWHERE k < 4294967295;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_noidx PREWHERE k < 4294967295;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM src WHERE k < 4294967295;

SELECT 'all but one row';
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_idx PREWHERE k != 2345;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_noidx PREWHERE k != 2345;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM src WHERE k != 2345;

SELECT 'half, scattered';
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_idx PREWHERE cityHash64(k) % 2 = 0;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_noidx PREWHERE cityHash64(k) % 2 = 0;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM src WHERE cityHash64(k) % 2 = 0;

SELECT '10%, scattered';
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_idx PREWHERE cityHash64(k) % 10 = 0;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_noidx PREWHERE cityHash64(k) % 10 = 0;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM src WHERE cityHash64(k) % 10 = 0;

SELECT '90%, scattered';
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_idx PREWHERE cityHash64(k) % 10 != 0;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_noidx PREWHERE cityHash64(k) % 10 != 0;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM src WHERE cityHash64(k) % 10 != 0;

SELECT '3%, scattered';
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_idx PREWHERE cityHash64(k) % 100 < 3;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_noidx PREWHERE cityHash64(k) % 100 < 3;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM src WHERE cityHash64(k) % 100 < 3;

SELECT '0.1%';
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_idx PREWHERE k % 1000 = 0;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_noidx PREWHERE k % 1000 = 0;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM src WHERE k % 1000 = 0;

SELECT 'clusters of 300 rows, half of the rows in each';
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_idx PREWHERE intDiv(k, 300) % 2 = 0 AND cityHash64(k) % 2 = 0;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_noidx PREWHERE intDiv(k, 300) % 2 = 0 AND cityHash64(k) % 2 = 0;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM src WHERE intDiv(k, 300) % 2 = 0 AND cityHash64(k) % 2 = 0;

SELECT 'a range of k, half of the rows in it';
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_idx PREWHERE k BETWEEN 1200 AND 3800 AND cityHash64(k) % 2 = 0;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_noidx PREWHERE k BETWEEN 1200 AND 3800 AND cityHash64(k) % 2 = 0;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM src WHERE k BETWEEN 1200 AND 3800 AND cityHash64(k) % 2 = 0;

SELECT 'UInt8 filter with values above 1';
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_idx PREWHERE x;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_noidx PREWHERE x;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM src WHERE x;

SELECT 'two steps, the second reads only the dictionary-encoded column';
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_idx PREWHERE cityHash64(k) % 2 = 0 AND c_dict > 10;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM pq_noidx PREWHERE cityHash64(k) % 2 = 0 AND c_dict > 10;
SELECT count(), sum(cityHash64(formatRow('TSV', *))) FROM src WHERE cityHash64(k) % 2 = 0 AND c_dict > 10;

SELECT 'nullable columns only';
SELECT count(), sum(cityHash64(formatRow('TSV', n_i32, n_d128))) FROM pq_idx PREWHERE cityHash64(k) % 2 = 0;
SELECT count(), sum(cityHash64(formatRow('TSV', n_i32, n_d128))) FROM pq_noidx PREWHERE cityHash64(k) % 2 = 0;
SELECT count(), sum(cityHash64(formatRow('TSV', n_i32, n_d128))) FROM src WHERE cityHash64(k) % 2 = 0;

SELECT 'a value the read type cannot hold, only in rows filtered out';
INSERT INTO FUNCTION file(currentDatabase() || '_05457_dates.parquet')
SELECT number AS k, if(number % 2 = 0, toDate32('2000-01-01'), toDate32('1960-01-01')) + intDiv(number, 2) AS d
FROM numbers(5000)
SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_max_dictionary_size = 1024;
SELECT count(), min(d), max(d) FROM file(currentDatabase() || '_05457_dates.parquet', Parquet, 'k UInt64, d Date') PREWHERE k % 2 = 0;

DROP TABLE pq_idx;
DROP TABLE pq_noidx;
DROP TABLE src;
