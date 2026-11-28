-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: Parquet, and MinIO via s3_conn.
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- Runtime filters would skip target data too
SET enable_join_runtime_filters = 0;
-- Under parallel replicas the reads are remote and the pass bails
SET enable_parallel_replicas = 0;
-- The copy is made only when the reader prunes row groups by min/max statistics
SET input_format_parquet_filter_push_down = 1;
SET engine_file_truncate_on_insert = 1, s3_truncate_on_insert = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS src;
CREATE TABLE src (k Int64) ENGINE = Memory;
INSERT INTO src SELECT number FROM numbers(100000);

INSERT INTO FUNCTION file(currentDatabase() || '_05497.parquet', Parquet, 'k Int64')
SELECT number FROM numbers(100000) SETTINGS output_format_parquet_row_group_size = 10000;
INSERT INTO FUNCTION s3(s3_conn, filename = '05497/' || currentDatabase() || '.parquet', format = Parquet, structure = 'k Int64')
SELECT number FROM numbers(100000) SETTINGS output_format_parquet_row_group_size = 10000;
INSERT INTO FUNCTION file(currentDatabase() || '_05497.csv', CSV, 'k Int64') SELECT number FROM numbers(100);

-- 1 occurrence = source side only, 2 = copied to the target too

SELECT 'file', countIf(explain LIKE '%ilter column:%k = 5%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src WHERE k = 5) AS s INNER JOIN file(currentDatabase() || '_05497.parquet', Parquet, 'k Int64') AS t ON s.k = t.k);

SELECT 'file result', count() FROM (SELECT * FROM src WHERE k = 5) AS s INNER JOIN file(currentDatabase() || '_05497.parquet', Parquet, 'k Int64') AS t ON s.k = t.k
SETTINGS log_comment = '05497_file';

SELECT 'file IN', countIf(explain LIKE '%ilter column:%k IN (5, 50000)%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src WHERE k IN (5, 50000)) AS s INNER JOIN file(currentDatabase() || '_05497.parquet', Parquet, 'k Int64') AS t ON s.k = t.k);
SELECT 'file IN result', count() FROM (SELECT * FROM src WHERE k IN (5, 50000)) AS s INNER JOIN file(currentDatabase() || '_05497.parquet', Parquet, 'k Int64') AS t ON s.k = t.k
SETTINGS log_comment = '05497_file_in';

SELECT 's3', countIf(explain LIKE '%ilter column:%k = 5%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src WHERE k = 5) AS s
    INNER JOIN s3(s3_conn, filename = '05497/' || currentDatabase() || '.parquet', format = Parquet, structure = 'k Int64') AS t ON s.k = t.k);
SELECT 's3 result', count() FROM (SELECT * FROM src WHERE k = 5) AS s
    INNER JOIN s3(s3_conn, filename = '05497/' || currentDatabase() || '.parquet', format = Parquet, structure = 'k Int64') AS t ON s.k = t.k
SETTINGS log_comment = '05497_s3', enable_filesystem_cache = 0;

SELECT 'url', countIf(explain LIKE '%ilter column:%k = 5%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src WHERE k = 5) AS s
    INNER JOIN url('http://localhost:11111/test/05497/' || currentDatabase() || '.parquet', Parquet, 'k Int64') AS t ON s.k = t.k);
SELECT 'url result', count() FROM (SELECT * FROM src WHERE k = 5) AS s
    INNER JOIN url('http://localhost:11111/test/05497/' || currentDatabase() || '.parquet', Parquet, 'k Int64') AS t ON s.k = t.k
SETTINGS log_comment = '05497_url', enable_filesystem_cache = 0;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ParquetPrunedRowGroups'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('05497_file', '05497_file_in', '05497_s3', '05497_url') AND type = 'QueryFinish'
ORDER BY log_comment;

SELECT 'csv', countIf(explain LIKE '%ilter column:%k = 5%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src WHERE k = 5) AS s INNER JOIN file(currentDatabase() || '_05497.csv', CSV, 'k Int64') AS t ON s.k = t.k);

SELECT 'filter push-down off', countIf(explain LIKE '%ilter column:%k = 5%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src WHERE k = 5) AS s INNER JOIN file(currentDatabase() || '_05497.parquet', Parquet, 'k Int64') AS t ON s.k = t.k
    SETTINGS input_format_parquet_filter_push_down = 0);

SELECT 'use_primary_key = 0', countIf(explain LIKE '%ilter column:%k = 5%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src WHERE k = 5) AS s INNER JOIN file(currentDatabase() || '_05497.parquet', Parquet, 'k Int64') AS t ON s.k = t.k
    SETTINGS use_primary_key = 0);

-- Parquet bounds leave out NaN, so `x != 1.5` would skip the row group holding only NaN and 1.5
DROP TABLE IF EXISTS src_nan;
CREATE TABLE src_nan (x Float64) ENGINE = Memory;
INSERT INTO src_nan VALUES (nan);
INSERT INTO FUNCTION file(currentDatabase() || '_05497_nan.parquet', Parquet, 'x Float64') SELECT if(number < 500, nan, 1.5) FROM numbers(1000);
SELECT 'nan', countIf(explain LIKE '%ilter column:%x != 1.5%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src_nan WHERE x != 1.5) AS s INNER JOIN file(currentDatabase() || '_05497_nan.parquet', Parquet, 'x Float64') AS t ON s.x = t.x);
SELECT 'nan result', count() FROM (SELECT * FROM src_nan WHERE x != 1.5) AS s INNER JOIN file(currentDatabase() || '_05497_nan.parquet', Parquet, 'x Float64') AS t ON s.x = t.x;

-- The default replaces NULL after the reader skipped data
DROP TABLE IF EXISTS src_def;
CREATE TABLE src_def (k Int64) ENGINE = Memory;
INSERT INTO src_def VALUES (-1);
INSERT INTO FUNCTION file(currentDatabase() || '_05497_def.parquet', Parquet, 'k Nullable(Int64)') SELECT if(number < 500, NULL, number) FROM numbers(1000);
SELECT 'default', countIf(explain LIKE '%ilter column:%k = -1%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src_def WHERE k = -1) AS s INNER JOIN file(currentDatabase() || '_05497_def.parquet', Parquet, 'k Int64 DEFAULT -1') AS t ON s.k = t.k);
SELECT 'default result', count() FROM (SELECT * FROM src_def WHERE k = -1) AS s INNER JOIN file(currentDatabase() || '_05497_def.parquet', Parquet, 'k Int64 DEFAULT -1') AS t ON s.k = t.k;

-- Hive partition columns come from the path
INSERT INTO FUNCTION file(currentDatabase() || '_05497_hive/key=9/data.parquet', Parquet, 'v Int64') SELECT number FROM numbers(10);
SELECT 'hive', countIf(explain LIKE '%ilter column:%k = 9%' OR explain LIKE '%ilter column:%key = 9%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src WHERE k = 9) AS s
    INNER JOIN file(currentDatabase() || '_05497_hive/key=*/data.parquet', Parquet) AS t ON s.k = t.key
    SETTINGS use_hive_partitioning = 1);
-- With a structure of only Hive partition columns (the path has one more key), the format reads them from the file
INSERT INTO FUNCTION file(currentDatabase() || '_05497_hive_only/a=1/key=9/data.parquet', Parquet, 'key Int64') SELECT number FROM numbers(10);
SELECT 'hive only', countIf(explain LIKE '%ilter column:%k = 9%' OR explain LIKE '%ilter column:%key = 9%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src WHERE k = 9) AS s
    INNER JOIN file(currentDatabase() || '_05497_hive_only/a=1/key=9/data.parquet', Parquet, 'key Int64') AS t ON s.k = t.key
    SETTINGS use_hive_partitioning = 1);

-- Only types whose statistics cover every value are targets
DROP TABLE IF EXISTS src_types;
CREATE TABLE src_types (
    c_u8 UInt8, c_date Date, c_date32 Date32, c_dec Decimal(9, 2), c_ni64 Nullable(Int64), c_lcni64 LowCardinality(Nullable(Int64)),
    c_str String, c_dt DateTime, c_uuid UUID, c_fs FixedString(8), c_nf32 Nullable(Float32), c_i128 Int128) ENGINE = Memory;
INSERT INTO src_types VALUES (1, '2020-01-02', '2020-01-02', 1.5, 5, 5, 'abc', '2020-01-02 00:00:00', '00000000-0000-0000-0000-000000000001', 'abc', 1.5, 5);
INSERT INTO FUNCTION file(currentDatabase() || '_05497_types.parquet', Parquet) SELECT * FROM src_types;

SELECT 'UInt8', countIf(explain LIKE '%ilter column:%c_u8%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src_types WHERE c_u8 = 1) AS s
    INNER JOIN file(currentDatabase() || '_05497_types.parquet', Parquet, 'c_u8 UInt8') AS t ON s.c_u8 = t.c_u8);
SELECT 'Date', countIf(explain LIKE '%ilter column:%c_date%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src_types WHERE c_date = toDate('2020-01-02')) AS s
    INNER JOIN file(currentDatabase() || '_05497_types.parquet', Parquet, 'c_date Date') AS t ON s.c_date = t.c_date);
SELECT 'Date32', countIf(explain LIKE '%ilter column:%c_date32%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src_types WHERE c_date32 = toDate32('2020-01-02')) AS s
    INNER JOIN file(currentDatabase() || '_05497_types.parquet', Parquet, 'c_date32 Date32') AS t ON s.c_date32 = t.c_date32);
SELECT 'Decimal', countIf(explain LIKE '%ilter column:%c_dec%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src_types WHERE c_dec = toDecimal32(1.5, 2)) AS s
    INNER JOIN file(currentDatabase() || '_05497_types.parquet', Parquet, 'c_dec Decimal(9, 2)') AS t ON s.c_dec = t.c_dec);
SELECT 'Nullable(Int64)', countIf(explain LIKE '%ilter column:%c_ni64%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src_types WHERE c_ni64 = 5) AS s
    INNER JOIN file(currentDatabase() || '_05497_types.parquet', Parquet, 'c_ni64 Nullable(Int64)') AS t ON s.c_ni64 = t.c_ni64);
SELECT 'LowCardinality(Nullable(Int64))', countIf(explain LIKE '%ilter column:%c_lcni64%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src_types WHERE c_lcni64 = 5) AS s
    INNER JOIN file(currentDatabase() || '_05497_types.parquet', Parquet, 'c_lcni64 LowCardinality(Nullable(Int64))') AS t ON s.c_lcni64 = t.c_lcni64);
SELECT 'String', countIf(explain LIKE '%ilter column:%c_str%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src_types WHERE c_str = 'abc') AS s
    INNER JOIN file(currentDatabase() || '_05497_types.parquet', Parquet, 'c_str String') AS t ON s.c_str = t.c_str);
SELECT 'DateTime', countIf(explain LIKE '%ilter column:%c_dt%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src_types WHERE c_dt = toDateTime('2020-01-02 00:00:00')) AS s
    INNER JOIN file(currentDatabase() || '_05497_types.parquet', Parquet, 'c_dt DateTime') AS t ON s.c_dt = t.c_dt);
SELECT 'UUID', countIf(explain LIKE '%ilter column:%c_uuid%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src_types WHERE c_uuid = toUUID('00000000-0000-0000-0000-000000000001')) AS s
    INNER JOIN file(currentDatabase() || '_05497_types.parquet', Parquet, 'c_uuid UUID') AS t ON s.c_uuid = t.c_uuid);
SELECT 'FixedString', countIf(explain LIKE '%ilter column:%c_fs%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src_types WHERE c_fs = toFixedString('abc', 8)) AS s
    INNER JOIN file(currentDatabase() || '_05497_types.parquet', Parquet, 'c_fs FixedString(8)') AS t ON s.c_fs = t.c_fs);
SELECT 'Nullable(Float32)', countIf(explain LIKE '%ilter column:%c_nf32%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src_types WHERE c_nf32 = toFloat32(1.5)) AS s
    INNER JOIN file(currentDatabase() || '_05497_types.parquet', Parquet, 'c_nf32 Nullable(Float32)') AS t ON s.c_nf32 = t.c_nf32);
SELECT 'Int128', countIf(explain LIKE '%ilter column:%c_i128%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src_types WHERE c_i128 = toInt128(5)) AS s
    INNER JOIN file(currentDatabase() || '_05497_types.parquet', Parquet, 'c_i128 Int128') AS t ON s.c_i128 = t.c_i128);

-- Each allowed type skips row groups by its statistics: row group i holds only the value i
INSERT INTO FUNCTION file(currentDatabase() || '_05497_types_rg.parquet', Parquet,
    'c_u8 UInt8, c_date Date, c_date32 Date32, c_dec Decimal(9, 2), c_ni64 Nullable(Int64), c_lcni64 LowCardinality(Nullable(Int64))')
SELECT intDiv(number, 1000), toDate('2020-01-01') + intDiv(number, 1000), toDate32('2020-01-01') + intDiv(number, 1000),
    toDecimal32(intDiv(number, 1000) + 0.5, 2), intDiv(number, 1000), intDiv(number, 1000)
FROM numbers(10000) SETTINGS output_format_parquet_row_group_size = 1000;
SELECT 'UInt8 result', count() FROM (SELECT * FROM src_types WHERE c_u8 = 1) AS s
    INNER JOIN file(currentDatabase() || '_05497_types_rg.parquet', Parquet, 'c_u8 UInt8') AS t ON s.c_u8 = t.c_u8
SETTINGS log_comment = '05497_t_u8';
SELECT 'Date result', count() FROM (SELECT * FROM src_types WHERE c_date = toDate('2020-01-02')) AS s
    INNER JOIN file(currentDatabase() || '_05497_types_rg.parquet', Parquet, 'c_date Date') AS t ON s.c_date = t.c_date
SETTINGS log_comment = '05497_t_date';
SELECT 'Date32 result', count() FROM (SELECT * FROM src_types WHERE c_date32 = toDate32('2020-01-02')) AS s
    INNER JOIN file(currentDatabase() || '_05497_types_rg.parquet', Parquet, 'c_date32 Date32') AS t ON s.c_date32 = t.c_date32
SETTINGS log_comment = '05497_t_date32';
SELECT 'Decimal result', count() FROM (SELECT * FROM src_types WHERE c_dec = toDecimal32(1.5, 2)) AS s
    INNER JOIN file(currentDatabase() || '_05497_types_rg.parquet', Parquet, 'c_dec Decimal(9, 2)') AS t ON s.c_dec = t.c_dec
SETTINGS log_comment = '05497_t_dec';
SELECT 'Nullable(Int64) result', count() FROM (SELECT * FROM src_types WHERE c_ni64 = 5) AS s
    INNER JOIN file(currentDatabase() || '_05497_types_rg.parquet', Parquet, 'c_ni64 Nullable(Int64)') AS t ON s.c_ni64 = t.c_ni64
SETTINGS log_comment = '05497_t_ni64';
SELECT 'LowCardinality(Nullable(Int64)) result', count() FROM (SELECT * FROM src_types WHERE c_lcni64 = 5) AS s
    INNER JOIN file(currentDatabase() || '_05497_types_rg.parquet', Parquet, 'c_lcni64 LowCardinality(Nullable(Int64))') AS t ON s.c_lcni64 = t.c_lcni64
SETTINGS log_comment = '05497_t_lcni64';
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ParquetPrunedRowGroups'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment IN ('05497_t_u8', '05497_t_date', '05497_t_date32', '05497_t_dec', '05497_t_ni64', '05497_t_lcni64')
    AND type = 'QueryFinish'
ORDER BY log_comment;

-- The format of a data lake is only the format new files are written in
SET allow_insert_into_iceberg = 1, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 0, async_insert = 0;
CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05497_iceberg/') AS path
FROM system.server_settings WHERE name = 'user_files_path';
DROP TABLE IF EXISTS ice;
CREATE TABLE ice (k Int64) ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet');
INSERT INTO ice SELECT number FROM numbers(100);
SELECT 'iceberg', countIf(explain LIKE '%ilter column:%k = 5%') FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM (SELECT * FROM src WHERE k = 5) AS s INNER JOIN ice AS t ON s.k = t.k);

DROP TABLE ice;
DROP TABLE src;
DROP TABLE src_nan;
DROP TABLE src_def;
DROP TABLE src_types;
