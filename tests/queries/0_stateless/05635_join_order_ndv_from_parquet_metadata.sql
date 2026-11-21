-- Tags: no-fasttest
-- Tag no-fasttest: needs S3

-- A read of plain Parquet files is estimated from their footers for join reordering,
-- see the setting `use_parquet_metadata_statistics`.

SET use_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_randomize = 0;
SET s3_truncate_on_insert = 1;

-- `d` is dictionary-encoded with 300 values in every row group; `u` is unique.
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05635/data.parquet', format = Parquet, structure = 'id UInt64, d String, u String')
SELECT number, 'value ' || toString(intHash32(number) % 300), toString(intHash64(number)) FROM numbers(100000)
SETTINGS output_format_parquet_row_group_size = 20000, output_format_parquet_use_custom_encoder = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt64, d String, u String) ENGINE = S3(s3_conn, filename = currentDatabase() || '/05635/data.parquet', format = Parquet);

SELECT 'The rows of the files';
SELECT extract(arrayStringConcat(groupArray(explain), '\n'), 'Left: rows estimated ([^\n]+)')
FROM (EXPLAIN SELECT count() FROM t AS a JOIN t AS b ON a.d = b.d) SETTINGS use_parquet_metadata_statistics = 1;

SELECT 'A dictionary-encoded column: the number of values of the dictionary pages';
SELECT round(1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')))
FROM (EXPLAIN SELECT count() FROM t AS a JOIN t AS b ON a.d = b.d) SETTINGS use_parquet_metadata_statistics = 1;

SELECT 'Without the setting: no estimate';
SELECT extract(arrayStringConcat(groupArray(explain), '\n'), 'Left: rows estimated ([^\n]+)')
FROM (EXPLAIN SELECT count() FROM t AS a JOIN t AS b ON a.d = b.d) SETTINGS use_parquet_metadata_statistics = 0;

SELECT 'A unique column: its row groups have as many distinct values as rows, so the column has about as many as the file';
SELECT 1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) >= 50000
FROM (EXPLAIN SELECT count() FROM t AS a JOIN t AS b ON a.u = b.u) SETTINGS use_parquet_metadata_statistics = 1;

DROP TABLE t;
