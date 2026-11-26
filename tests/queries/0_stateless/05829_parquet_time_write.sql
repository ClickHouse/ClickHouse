-- Tags: no-fasttest

-- Write `Time`/`Time64` as Parquet `TIME` and validate the physical schema,
-- inferred types, and values read into explicitly declared types.
SET engine_file_truncate_on_insert = 1;
SET schema_inference_make_columns_nullable = 0;
SET use_query_condition_cache = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_05829_types.parquet', Parquet)
SELECT CAST('23:59:59', 'Time') AS t,
       CAST('23:59:59', 'Time64(0)') AS t0,
       CAST('23:59:59.1', 'Time64(1)') AS t1,
       CAST('23:59:59.12', 'Time64(2)') AS t2,
       CAST('23:59:59.123', 'Time64(3)') AS t3,
       CAST('23:59:59.1234', 'Time64(4)') AS t4,
       CAST('23:59:59.12345', 'Time64(5)') AS t5,
       CAST('23:59:59.123456', 'Time64(6)') AS t6,
       CAST('23:59:59.1234567', 'Time64(7)') AS t7,
       CAST('23:59:59.12345678', 'Time64(8)') AS t8,
       CAST('23:59:59.123456789', 'Time64(9)') AS t9;
SELECT c.name, c.physical_type, c.logical_type
FROM file(currentDatabase() || '_05829_types.parquet', ParquetMetadata)
ARRAY JOIN columns AS c;

DESCRIBE TABLE file(currentDatabase() || '_05829_types.parquet');

-- Read into explicitly declared types, as an S3 table or restore would do.
SELECT toString(t), toString(t0), toString(t1), toString(t2), toString(t3), toString(t4), toString(t5), toString(t6), toString(t7), toString(t8), toString(t9)
FROM file(currentDatabase() || '_05829_types.parquet', Parquet,
    't Time, t0 Time64(0), t1 Time64(1), t2 Time64(2), t3 Time64(3), t4 Time64(4), t5 Time64(5), t6 Time64(6), t7 Time64(7), t8 Time64(8), t9 Time64(9)');

-- Validate values and statistics with both dictionary and plain encoding.
SET output_format_parquet_max_dictionary_size = 1048576;
SET output_format_parquet_row_group_size = 1000;
INSERT INTO FUNCTION file(currentDatabase() || '_05829_stats.parquet', Parquet)
SELECT toTime64(number, 3) AS t3, toTime64(number, 6) AS t6, toTime64(number, 9) AS t9 FROM numbers(10000);
SELECT count(), min(t3), max(t9)
FROM file(currentDatabase() || '_05829_stats.parquet', Parquet)
WHERE t3 >= toTime64(1234, 3) AND t6 < toTime64(5678, 6)
SETTINGS input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;
SELECT count(), min(t3), max(t9)
FROM file(currentDatabase() || '_05829_stats.parquet', Parquet)
WHERE t3 >= toTime64(1234, 3) AND t6 < toTime64(5678, 6)
SETTINGS input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 1;
SET output_format_parquet_max_dictionary_size = 0;
SET output_format_parquet_row_group_size = 1000;
INSERT INTO FUNCTION file(currentDatabase() || '_05829_stats.parquet', Parquet)
SELECT toTime64(number, 3) AS t3, toTime64(number, 6) AS t6, toTime64(number, 9) AS t9 FROM numbers(10000);
SELECT count(), min(t3), max(t9)
FROM file(currentDatabase() || '_05829_stats.parquet', Parquet)
WHERE t3 >= toTime64(1234, 3) AND t6 < toTime64(5678, 6)
SETTINGS input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0;
SELECT count(), min(t3), max(t9)
FROM file(currentDatabase() || '_05829_stats.parquet', Parquet)
WHERE t3 >= toTime64(1234, 3) AND t6 < toTime64(5678, 6)
SETTINGS input_format_parquet_filter_push_down = 1, input_format_parquet_page_filter_push_down = 1;

-- Real nulls and nested values must reach the same primitive conversion.
INSERT INTO FUNCTION file(currentDatabase() || '_05829_nulls.parquet', Parquet)
SELECT CAST(if(number = 0, NULL, '12:30:00.123'), 'Nullable(Time64(3))') AS t,
       CAST(['00:00:00.000001', NULL, '23:59:59.999999'], 'Array(Nullable(Time64(6)))') AS a
FROM numbers(2);
SELECT count(), count(t), max(t) FROM file(currentDatabase() || '_05829_nulls.parquet', Parquet);
SELECT a FROM file(currentDatabase() || '_05829_nulls.parquet', Parquet) WHERE t IS NOT NULL;

-- Valid endpoints of the standard time-of-day domain.
INSERT INTO FUNCTION file(currentDatabase() || '_05829_endpoints.parquet', Parquet)
SELECT toTime(0) AS t, toTime64('23:59:59.999', 3) AS t3,
       toTime64('23:59:59.999999', 6) AS t6, toTime64('23:59:59.999999999', 9) AS t9;
SELECT t, t3, t6, t9 FROM file(currentDatabase() || '_05829_endpoints.parquet', Parquet,
    't Time, t3 Time64(3), t6 Time64(6), t9 Time64(9)');

SET schema_inference_make_columns_nullable = 3;

-- Nullable `Time` and `Time64` preserve their inferred types.
INSERT INTO FUNCTION file(currentDatabase() || '_05829_nullable.parquet', Parquet, 't Nullable(Time), t64 Nullable(Time64(3))')
    SELECT cast(toTime(86399), 'Nullable(Time)'), cast(toTime64('23:59:59.999', 3), 'Nullable(Time64(3))');
DESCRIBE TABLE file(currentDatabase() || '_05829_nullable.parquet');
SELECT toString(t), toString(t64) FROM file(currentDatabase() || '_05829_nullable.parquet');

-- Min/max statistics must hold parquet physical values (milliseconds) so that
-- reading the file back with a predicate prunes by stats.
SET output_format_parquet_row_group_size = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05829_time_stats.parquet', Parquet, 't Time')
    SELECT toTime(number) FROM numbers(10);
SELECT count() FROM file(currentDatabase() || '_05829_time_stats.parquet') WHERE t > toTime(4);

-- A `Time` value outside the Parquet time-of-day range must be rejected.
SELECT cast(3000000, 'Time') AS t FORMAT Parquet; -- { clientError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
