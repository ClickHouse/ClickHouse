-- A `minmax` index is analyzed through integer conversions that change the signedness, convert into `DateTime` or `Bool`,
-- or convert `Nullable` integers. Each query compares the result with the index against a full scan.

DROP TABLE IF EXISTS t_minmax_integer_cast;

CREATE TABLE t_minmax_integer_cast
(
    u64 UInt64,
    i64 Int64,
    i32 Int32,
    dt DateTime('UTC'),
    u8 UInt8,
    nu64 Nullable(UInt64),
    ni64 Nullable(Int64),
    u64_dt DateTime('UTC') ALIAS u64,
    INDEX idx_u64 u64 TYPE minmax GRANULARITY 1,
    INDEX idx_i64 i64 TYPE minmax GRANULARITY 1,
    INDEX idx_i32 i32 TYPE minmax GRANULARITY 1,
    INDEX idx_dt dt TYPE minmax GRANULARITY 1,
    INDEX idx_u8 u8 TYPE minmax GRANULARITY 1,
    INDEX idx_nu64 nu64 TYPE minmax GRANULARITY 1,
    INDEX idx_ni64 ni64 TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 16;

-- u64 crosses the `DateTime` maximum 4294967295, i64 goes from below 0 to above it, i32 crosses 0, dt crosses 2^31.
INSERT INTO t_minmax_integer_cast
SELECT
    4294000000 + number * 97,
    (toInt64(number) - 10000) * 430000,
    toInt32(number * 97) - 1000000,
    toDateTime(2147000000 + number * 97, 'UTC'),
    intDiv(number, 100) % 4,
    if(number % 1000 < 2, NULL, number * 97 + 65000),
    if(number % 1000 < 2, NULL, toInt64(number) - 10000)
FROM numbers(20000);

SELECT 'UInt64 to DateTime';
SELECT count() FROM t_minmax_integer_cast WHERE u64_dt BETWEEN '2106-02-07 06:00:00' AND '2106-02-07 06:10:00';
SELECT count() FROM t_minmax_integer_cast WHERE u64_dt BETWEEN '2106-02-07 06:00:00' AND '2106-02-07 06:10:00' SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast WHERE u64_dt BETWEEN '2106-02-07 06:00:00' AND '2106-02-07 06:10:00' SETTINGS force_data_skipping_indices = 'idx_u64', max_rows_to_read = 100;
-- Every value above the maximum saturates to it.
SELECT count() FROM t_minmax_integer_cast WHERE CAST(u64, 'DateTime(\'UTC\')') = '2106-02-07 06:28:15';
SELECT count() FROM t_minmax_integer_cast WHERE CAST(u64, 'DateTime(\'UTC\')') = '2106-02-07 06:28:15' SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;

SELECT 'Int64 to DateTime';
-- Every negative value saturates to 0.
SELECT count() FROM t_minmax_integer_cast WHERE toDateTime(i64, 'UTC') = '1970-01-01 00:00:00';
SELECT count() FROM t_minmax_integer_cast WHERE toDateTime(i64, 'UTC') = '1970-01-01 00:00:00' SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast WHERE toDateTime(i64, 'UTC') BETWEEN '2024-01-01 00:00:00' AND '2024-01-08 00:00:00';
SELECT count() FROM t_minmax_integer_cast WHERE toDateTime(i64, 'UTC') BETWEEN '2024-01-01 00:00:00' AND '2024-01-08 00:00:00' SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast WHERE toDateTime(i64, 'UTC') BETWEEN '2024-01-01 00:00:00' AND '2024-01-08 00:00:00' SETTINGS force_data_skipping_indices = 'idx_i64', max_rows_to_read = 100;
SELECT count() FROM t_minmax_integer_cast WHERE toDateTime(i64, 'UTC') = '2106-02-07 06:28:15';
SELECT count() FROM t_minmax_integer_cast WHERE toDateTime(i64, 'UTC') = '2106-02-07 06:28:15' SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast WHERE toDateTime(i64, 'UTC') = '1970-01-01 00:00:00' SETTINGS date_time_overflow_behavior = 'saturate';
SELECT count() FROM t_minmax_integer_cast WHERE toDateTime(i64, 'UTC') = '1970-01-01 00:00:00' SETTINGS date_time_overflow_behavior = 'saturate', use_skip_indexes = 0, use_statistics_for_part_pruning = 0;

SELECT 'Int32 to DateTime';
SELECT count() FROM t_minmax_integer_cast WHERE CAST(i32, 'DateTime(\'UTC\')') = '1970-01-01 00:00:00';
SELECT count() FROM t_minmax_integer_cast WHERE CAST(i32, 'DateTime(\'UTC\')') = '1970-01-01 00:00:00' SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast WHERE CAST(i32, 'DateTime(\'UTC\')') BETWEEN '1970-01-01 01:00:00' AND '1970-01-01 02:00:00';
SELECT count() FROM t_minmax_integer_cast WHERE CAST(i32, 'DateTime(\'UTC\')') BETWEEN '1970-01-01 01:00:00' AND '1970-01-01 02:00:00' SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;

SELECT 'signed to unsigned';
-- A negative value wraps around to the top of the unsigned range.
SELECT count() FROM t_minmax_integer_cast WHERE CAST(i64, 'UInt64') >= 18446744000000000000;
SELECT count() FROM t_minmax_integer_cast WHERE CAST(i64, 'UInt64') >= 18446744000000000000 SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast WHERE toUInt32(i64) BETWEEN 1000000000 AND 1100000000;
SELECT count() FROM t_minmax_integer_cast WHERE toUInt32(i64) BETWEEN 1000000000 AND 1100000000 SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast WHERE toUInt64(i32) > 18446744073709000000;
SELECT count() FROM t_minmax_integer_cast WHERE toUInt64(i32) > 18446744073709000000 SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast WHERE toUnixTimestamp(i64) BETWEEN 1000000000 AND 1100000000;
SELECT count() FROM t_minmax_integer_cast WHERE toUnixTimestamp(i64) BETWEEN 1000000000 AND 1100000000 SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;

SELECT 'unsigned to signed';
SELECT count() FROM t_minmax_integer_cast WHERE CAST(u64, 'Int32') < 0;
SELECT count() FROM t_minmax_integer_cast WHERE CAST(u64, 'Int32') < 0 SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast WHERE toInt32(dt) < 0;
SELECT count() FROM t_minmax_integer_cast WHERE toInt32(dt) < 0 SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast WHERE toInt64(dt) < 2147100000;
SELECT count() FROM t_minmax_integer_cast WHERE toInt64(dt) < 2147100000 SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast WHERE toInt64(dt) < 2147100000 SETTINGS force_data_skipping_indices = 'idx_dt', max_rows_to_read = 1100;

SELECT 'Nullable';
SELECT count() FROM t_minmax_integer_cast WHERE CAST(nu64, 'Nullable(UInt16)') BETWEEN 1000 AND 1100;
SELECT count() FROM t_minmax_integer_cast WHERE CAST(nu64, 'Nullable(UInt16)') BETWEEN 1000 AND 1100 SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast WHERE toDateTime(ni64, 'UTC') = '1970-01-01 00:00:00';
SELECT count() FROM t_minmax_integer_cast WHERE toDateTime(ni64, 'UTC') = '1970-01-01 00:00:00' SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast WHERE toDateTime(ni64, 'UTC') BETWEEN '1970-01-01 01:00:00' AND '1970-01-01 01:10:00';
SELECT count() FROM t_minmax_integer_cast WHERE toDateTime(ni64, 'UTC') BETWEEN '1970-01-01 01:00:00' AND '1970-01-01 01:10:00' SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast WHERE toDateTime(ni64, 'UTC') BETWEEN '1970-01-01 01:00:00' AND '1970-01-01 01:10:00' SETTINGS force_data_skipping_indices = 'idx_ni64', max_rows_to_read = 1000;
SELECT count() FROM t_minmax_integer_cast WHERE CAST(ni64, 'Nullable(UInt64)') > 18446744073709551000;
SELECT count() FROM t_minmax_integer_cast WHERE CAST(ni64, 'Nullable(UInt64)') > 18446744073709551000 SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;

SELECT 'Bool';
-- Every nonzero value converts to true.
SELECT count() FROM t_minmax_integer_cast WHERE CAST(u8, 'Bool') = true;
SELECT count() FROM t_minmax_integer_cast WHERE CAST(u8, 'Bool') = true SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast WHERE CAST(u8, 'Bool') = false;
SELECT count() FROM t_minmax_integer_cast WHERE CAST(u8, 'Bool') = false SETTINGS use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast WHERE CAST(u8, 'Bool') = false SETTINGS force_data_skipping_indices = 'idx_u8', max_rows_to_read = 8000;

DROP TABLE t_minmax_integer_cast;

-- With `date_time_overflow_behavior = 'throw'` a value above the `DateTime` maximum throws, with the index too.
DROP TABLE IF EXISTS t_minmax_integer_cast_throw;

CREATE TABLE t_minmax_integer_cast_throw (u64 UInt64, INDEX idx_u64 u64 TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 16;

INSERT INTO t_minmax_integer_cast_throw SELECT 1700000000 + number * 1000000 FROM numbers(4000);

SELECT 'throw';
SELECT count() FROM t_minmax_integer_cast_throw WHERE toDateTime(u64, 'UTC') < '2000-01-01 00:00:00' SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT count() FROM t_minmax_integer_cast_throw WHERE toDateTime(u64, 'UTC') < '2000-01-01 00:00:00' SETTINGS date_time_overflow_behavior = 'throw', use_skip_indexes = 0, use_statistics_for_part_pruning = 0; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

DROP TABLE t_minmax_integer_cast_throw;

DROP TABLE IF EXISTS t_minmax_integer_cast_fit;

CREATE TABLE t_minmax_integer_cast_fit (i64 Int64, INDEX idx_i64 i64 TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 16;

INSERT INTO t_minmax_integer_cast_fit SELECT 1700000000 + number * 97 FROM numbers(20000);

SELECT count() FROM t_minmax_integer_cast_fit WHERE toDateTime(i64, 'UTC') BETWEEN '2023-11-14 22:20:00' AND '2023-11-14 22:30:00' SETTINGS date_time_overflow_behavior = 'throw';
SELECT count() FROM t_minmax_integer_cast_fit WHERE toDateTime(i64, 'UTC') BETWEEN '2023-11-14 22:20:00' AND '2023-11-14 22:30:00' SETTINGS date_time_overflow_behavior = 'throw', use_skip_indexes = 0, use_statistics_for_part_pruning = 0;
SELECT count() FROM t_minmax_integer_cast_fit WHERE toDateTime(i64, 'UTC') BETWEEN '2023-11-14 22:20:00' AND '2023-11-14 22:30:00' SETTINGS date_time_overflow_behavior = 'throw', force_data_skipping_indices = 'idx_i64', max_rows_to_read = 100;

DROP TABLE t_minmax_integer_cast_fit;
