-- A `minmax` index is analyzed through a narrowing integer conversion of the indexed column (e.g. `CAST(x, 'UInt16')`
-- of a `UInt64` column). The conversion wraps around, so the bounds of a granule are converted by keeping their low bits.
-- Each query compares the result with the index against a full scan.

DROP TABLE IF EXISTS t_minmax_narrowing_cast;

CREATE TABLE t_minmax_narrowing_cast
(
    u64 UInt64,
    i64 Int64,
    u32 UInt32,
    dt DateTime('UTC'),
    INDEX idx_u64 u64 TYPE minmax GRANULARITY 1,
    INDEX idx_i64 i64 TYPE minmax GRANULARITY 1,
    INDEX idx_u32 u32 TYPE minmax GRANULARITY 1,
    INDEX idx_dt dt TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 16;

-- The values cross many boundaries of the narrower types, so some granules convert to a continuous range and others wrap around.
INSERT INTO t_minmax_narrowing_cast
SELECT number * 97 + 65000, toInt64(number * 97) - 1000000, toUInt32(number * 97 + 4294000000), toDateTime(1700000000 + number * 97, 'UTC')
FROM numbers(20000);

SELECT 'u64 UInt16';
SELECT count() FROM t_minmax_narrowing_cast WHERE CAST(u64, 'UInt16') BETWEEN 1000 AND 1100;
SELECT count() FROM t_minmax_narrowing_cast WHERE CAST(u64, 'UInt16') BETWEEN 1000 AND 1100 SETTINGS use_skip_indexes = 0;
-- The index prunes the granules: only those around the matching rows and those that wrap around are read.
SELECT count() FROM t_minmax_narrowing_cast WHERE CAST(u64, 'UInt16') BETWEEN 1000 AND 1100 SETTINGS force_data_skipping_indices = 'idx_u64', max_rows_to_read = 2000;

SELECT 'u64 UInt8';
SELECT count() FROM t_minmax_narrowing_cast WHERE toUInt8(u64) = 7;
SELECT count() FROM t_minmax_narrowing_cast WHERE toUInt8(u64) = 7 SETTINGS use_skip_indexes = 0;

SELECT 'i64 Int16';
SELECT count() FROM t_minmax_narrowing_cast WHERE CAST(i64, 'Int16') >= -200 AND CAST(i64, 'Int16') < -100;
SELECT count() FROM t_minmax_narrowing_cast WHERE CAST(i64, 'Int16') >= -200 AND CAST(i64, 'Int16') < -100 SETTINGS use_skip_indexes = 0;

SELECT 'i64 Int8';
SELECT count() FROM t_minmax_narrowing_cast WHERE toInt8(i64) = -5;
SELECT count() FROM t_minmax_narrowing_cast WHERE toInt8(i64) = -5 SETTINGS use_skip_indexes = 0;

SELECT 'u32 UInt16';
SELECT count() FROM t_minmax_narrowing_cast WHERE CAST(u32, 'UInt16') BETWEEN 30000 AND 30500;
SELECT count() FROM t_minmax_narrowing_cast WHERE CAST(u32, 'UInt16') BETWEEN 30000 AND 30500 SETTINGS use_skip_indexes = 0;

SELECT 'DateTime UInt16';
SELECT count() FROM t_minmax_narrowing_cast WHERE CAST(dt, 'UInt16') BETWEEN 5000 AND 5200;
SELECT count() FROM t_minmax_narrowing_cast WHERE CAST(dt, 'UInt16') BETWEEN 5000 AND 5200 SETTINGS use_skip_indexes = 0;

DROP TABLE t_minmax_narrowing_cast;

-- A conversion to an 8-bit type is monotonic on a granule only when the granule does not cross a multiple of 256,
-- so these granules hold 4 consecutive values.
DROP TABLE IF EXISTS t_minmax_narrowing_cast_8bit;

CREATE TABLE t_minmax_narrowing_cast_8bit
(
    u64 UInt64,
    i64 Int64,
    INDEX idx_u64 u64 TYPE minmax GRANULARITY 1,
    INDEX idx_i64 i64 TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO t_minmax_narrowing_cast_8bit SELECT number + 65536, toInt64(number) - 10000 FROM numbers(20000);

SELECT 'u64 UInt8 pruned';
SELECT count() FROM t_minmax_narrowing_cast_8bit WHERE toUInt8(u64) = 7 SETTINGS force_data_skipping_indices = 'idx_u64', max_rows_to_read = 1000;
SELECT count() FROM t_minmax_narrowing_cast_8bit WHERE toUInt8(u64) = 7 SETTINGS use_skip_indexes = 0;

SELECT 'i64 Int8 pruned';
SELECT count() FROM t_minmax_narrowing_cast_8bit WHERE toInt8(i64) = -5 SETTINGS force_data_skipping_indices = 'idx_i64', max_rows_to_read = 1000;
SELECT count() FROM t_minmax_narrowing_cast_8bit WHERE toInt8(i64) = -5 SETTINGS use_skip_indexes = 0;

DROP TABLE t_minmax_narrowing_cast_8bit;

-- A conversion of a `UInt64` above the `UInt32` maximum to `DateTime` saturates instead of wrapping around, so it is not
-- applied to the bounds directly. Each part holds a single value, which makes its range a single point.
DROP TABLE IF EXISTS t_minmax_narrowing_cast_datetime;

CREATE TABLE t_minmax_narrowing_cast_datetime (u64 UInt64) ENGINE = MergeTree PARTITION BY u64 ORDER BY tuple();

-- 5000000000 saturates to 4294967295; wrapped around, it would be 705032704.
INSERT INTO t_minmax_narrowing_cast_datetime VALUES (5000000000), (4294967295), (705032704), (1000);

SELECT 'DateTime saturation';
SELECT u64 FROM t_minmax_narrowing_cast_datetime WHERE CAST(u64, 'DateTime(\'UTC\')') = '2106-02-07 06:28:15' ORDER BY u64;
SELECT u64 FROM t_minmax_narrowing_cast_datetime WHERE toDateTime(u64, 'UTC') = '2106-02-07 06:28:15' ORDER BY u64;
SELECT u64 FROM t_minmax_narrowing_cast_datetime WHERE toDateTime(u64, 'UTC') = '1992-05-05 02:25:04' ORDER BY u64;

DROP TABLE t_minmax_narrowing_cast_datetime;
