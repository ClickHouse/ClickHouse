-- Tags: no-fasttest
-- Tag no-fasttest: needs S3

-- The estimate of a read of plain Parquet files from their footers (`use_parquet_metadata_statistics`) keeps the number
-- of distinct values of a column whose column chunks have no NULL counts: a writer may omit the statistics of an
-- `OPTIONAL` column. The file was written by `pyarrow` with `write_statistics=False`: 20000 rows in 4 row groups,
-- `d` is `OPTIONAL`, dictionary-encoded, with 300 values in every row group and without statistics.

SET use_statistics = 1;
SET use_parquet_metadata_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_randomize = 0; -- Pinned because the test asserts on the estimates

SELECT 'The file has no NULL counts';
SELECT DISTINCT tupleElement(arrayJoin(arrayJoin(row_groups).columns), 'statistics').null_count
FROM s3(s3_conn, filename = '05895_optional_without_null_counts.parquet', format = ParquetMetadata);

SELECT 'The rows of the file';
SELECT extract(arrayStringConcat(groupArray(explain), '\n'), 'Left: rows estimated ([^\n]+)')
FROM (EXPLAIN SELECT count() FROM s3(s3_conn, filename = '05895_optional_without_null_counts.parquet', format = Parquet, structure = 'id Int64, d Nullable(String)') AS a
JOIN s3(s3_conn, filename = '05895_optional_without_null_counts.parquet', format = Parquet, structure = 'id Int64, d Nullable(String)') AS b ON a.d = b.d);

SELECT 'The number of values of the dictionary pages';
SELECT round(1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')))
FROM (EXPLAIN SELECT count() FROM s3(s3_conn, filename = '05895_optional_without_null_counts.parquet', format = Parquet, structure = 'id Int64, d Nullable(String)') AS a
JOIN s3(s3_conn, filename = '05895_optional_without_null_counts.parquet', format = Parquet, structure = 'id Int64, d Nullable(String)') AS b ON a.d = b.d);
