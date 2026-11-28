-- Tags: no-fasttest
-- Tag no-fasttest: needs S3

-- The estimate of a read of plain Parquet files from their footers (`use_parquet_metadata_statistics`) lists the files
-- with the format settings of the table, as the read does. The Hive partition `d=1900-01-01` of a `Date` column is
-- saturated by `date_time_overflow_behavior = 'ignore'` of the table, while the query has `throw`.

SET use_statistics = 1;
SET use_parquet_metadata_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET enable_parallel_replicas = 0;
SET s3_truncate_on_insert = 1;
SET use_hive_partitioning = 1;

INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05840/d=1900-01-01/data.parquet', format = Parquet, structure = 'id UInt64')
SELECT number FROM numbers(1000);
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '/05840/d=2020-01-01/data.parquet', format = Parquet, structure = 'id UInt64')
SELECT number FROM numbers(500);

DROP TABLE IF EXISTS t_05840;
CREATE TABLE t_05840 (id UInt64, d Date) ENGINE = S3(s3_conn, filename = currentDatabase() || '/05840/**/data.parquet', format = Parquet)
SETTINGS date_time_overflow_behavior = 'ignore';

SET date_time_overflow_behavior = 'throw';

SELECT 'The read';
SELECT count() FROM t_05840 AS a JOIN t_05840 AS b ON a.id = b.id WHERE a.d = '1970-01-01' AND b.d = '1970-01-01';

SELECT 'The estimate';
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_05840 AS a JOIN t_05840 AS b ON a.id = b.id WHERE a.d = '1970-01-01' AND b.d = '1970-01-01')
WHERE explain LIKE '%rows estimated%';

DROP TABLE t_05840;
