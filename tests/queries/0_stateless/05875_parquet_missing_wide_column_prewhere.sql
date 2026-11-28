-- Tags: no-fasttest
-- The size of a column that is not in the file is estimated without materializing its default value, before the
-- row groups are pruned and `PREWHERE` is applied: a default of `FixedString(12582912)` takes 12 MiB, more than the
-- memory limit, while no row of the file passes `PREWHERE`, so no default is ever needed. The same for a missing
-- element of a `Tuple`.

INSERT INTO FUNCTION file(currentDatabase() || '_05875.parquet')
SELECT number * 2 AS id, tuple(number) :: Tuple(a UInt64) AS t FROM numbers(1000)
SETTINGS engine_file_truncate_on_insert = 1;

SET input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_allow_missing_columns = 1,
    allow_suspicious_fixed_string_types = 1;

SELECT count(), sum(cityHash64(missing))
FROM file(currentDatabase() || '_05875.parquet', Parquet, 'id UInt64, missing FixedString(12582912)')
PREWHERE id % 2 = 1
SETTINGS max_memory_usage = '8Mi', use_columns_cache = 0;

SELECT count(), sum(cityHash64(missing))
FROM file(currentDatabase() || '_05875.parquet', Parquet, 'id UInt64, missing FixedString(12582912)')
PREWHERE id % 2 = 1
SETTINGS max_memory_usage = '8Mi', use_columns_cache = 1;

SELECT count(), sum(cityHash64(t))
FROM file(currentDatabase() || '_05875.parquet', Parquet, 'id UInt64, t Tuple(a UInt64, missing FixedString(12582912))')
PREWHERE id % 2 = 1
SETTINGS max_memory_usage = '8Mi', use_columns_cache = 0;

SELECT count(), sum(cityHash64(t))
FROM file(currentDatabase() || '_05875.parquet', Parquet, 'id UInt64, t Tuple(a UInt64, missing FixedString(12582912))')
PREWHERE id % 2 = 1
SETTINGS max_memory_usage = '8Mi', use_columns_cache = 1;
