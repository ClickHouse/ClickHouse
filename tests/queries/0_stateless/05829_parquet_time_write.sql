-- Tags: no-fasttest

-- Writing `Time`/`Time64` columns to Parquet must map to the Parquet `TIME` logical type
-- (INT32 MILLIS, INT64 MICROS/NANOS), symmetric to the
-- reader side which infers `Time64` from Parquet `TIME`.

SET engine_file_truncate_on_insert = 1;

-- `Time` (seconds) is written as TIME(MILLIS), so it comes back as Time64(3).
INSERT INTO FUNCTION file(currentDatabase() || '_05829_time.parquet', Parquet, 't Time')
    SELECT toTime(3723);
DESCRIBE TABLE file(currentDatabase() || '_05829_time.parquet');
SELECT toString(t) FROM file(currentDatabase() || '_05829_time.parquet');

-- Time64 with different scales: 9 -> NANOS, 6 -> MICROS, 3 -> MILLIS,
-- 0 and 5 are rescaled to the next finer Parquet unit.
INSERT INTO FUNCTION file(currentDatabase() || '_05829_time64.parquet', Parquet, 't9 Time64(9), t6 Time64(6), t3 Time64(3), t0 Time64(0), t5 Time64(5)')
    SELECT toTime64('01:02:03.456789012', 9), toTime64('01:02:03.456789', 6), toTime64('01:02:03.456', 3), toTime64('01:02:03', 0), toTime64('01:02:03.45678', 5);
DESCRIBE TABLE file(currentDatabase() || '_05829_time64.parquet');
SELECT toString(t9), toString(t6), toString(t3), toString(t0), toString(t5) FROM file(currentDatabase() || '_05829_time64.parquet');

-- Nullable wrapper.
INSERT INTO FUNCTION file(currentDatabase() || '_05829_nullable.parquet', Parquet, 't Nullable(Time), t64 Nullable(Time64(3))')
    SELECT cast(toTime(86399), 'Nullable(Time)'), cast(toTime64('23:59:59.999', 3), 'Nullable(Time64(3))');
DESCRIBE TABLE file(currentDatabase() || '_05829_nullable.parquet');
SELECT toString(t), toString(t64) FROM file(currentDatabase() || '_05829_nullable.parquet');

-- Min/max statistics must hold parquet physical values (milliseconds) so that
-- reading the file back with a predicate prunes by stats.
SET output_format_parquet_row_group_size = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05829_stats.parquet', Parquet, 't Time')
    SELECT toTime(number) FROM numbers(10);
SET use_query_condition_cache = 0;
SELECT count() FROM file(currentDatabase() || '_05829_stats.parquet') WHERE t > toTime(4);

-- A `Time` value whose milliseconds would overflow INT32 must be rejected.
SELECT cast(3000000, 'Time') AS t FORMAT Parquet; -- { clientError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
