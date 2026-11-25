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
