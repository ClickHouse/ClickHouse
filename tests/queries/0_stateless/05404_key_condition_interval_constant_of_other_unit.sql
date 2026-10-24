-- Index analysis converts an `Interval` constant of a coarser unit to the unit of an `Interval` key column.

SET enable_parallel_replicas = 0;
SET optimize_use_implicit_projections = 0;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_ns;
DROP TABLE IF EXISTS t_ms;
DROP TABLE IF EXISTS t_skip;
DROP TABLE IF EXISTS t_month;

CREATE TABLE t_ns (time IntervalNanosecond) ENGINE = MergeTree ORDER BY time
    SETTINGS index_granularity = 1000, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_ns SELECT toIntervalNanosecond(number * 1000000000) FROM numbers(100000);
OPTIMIZE TABLE t_ns FINAL;

CREATE TABLE t_ms (time IntervalMillisecond) ENGINE = MergeTree ORDER BY time
    SETTINGS index_granularity = 1000, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_ms SELECT toIntervalMillisecond(number * 1000) FROM numbers(100000);
OPTIMIZE TABLE t_ms FINAL;

CREATE TABLE t_skip
(
    id UInt64, time IntervalNanosecond, us IntervalMicrosecond, s IntervalSecond,
    INDEX mm time TYPE minmax GRANULARITY 1, INDEX mm_us us TYPE minmax GRANULARITY 1, INDEX mm_s s TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1000, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_skip SELECT number, toIntervalNanosecond(number * 1000000000), toIntervalMicrosecond(number * 1000000), toIntervalSecond(number)
    FROM numbers(100000);
OPTIMIZE TABLE t_skip FINAL;

CREATE TABLE t_month (m IntervalMonth) ENGINE = MergeTree ORDER BY m
    SETTINGS index_granularity = 10, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_month SELECT toIntervalMonth(number) FROM numbers(1000);
OPTIMIZE TABLE t_month FINAL;

SELECT 'ns between minute', count() FROM t_ns WHERE time BETWEEN INTERVAL 510 MINUTE AND INTERVAL 515 MINUTE;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ns WHERE time BETWEEN INTERVAL 510 MINUTE AND INTERVAL 515 MINUTE) WHERE explain ILIKE '%Condition:%' OR explain ILIKE '%Granules: %/%';
SELECT 'ns >= hour', count() FROM t_ns WHERE time >= INTERVAL 27 HOUR;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ns WHERE time >= INTERVAL 27 HOUR) WHERE explain ILIKE '%Condition:%' OR explain ILIKE '%Granules: %/%';
SELECT 'ns < day', count() FROM t_ns WHERE time < INTERVAL 1 DAY;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ns WHERE time < INTERVAL 1 DAY) WHERE explain ILIKE '%Condition:%' OR explain ILIKE '%Granules: %/%';
SELECT 'ns >= week', count() FROM t_ns WHERE time >= INTERVAL 1 WEEK;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ns WHERE time >= INTERVAL 1 WEEK) WHERE explain ILIKE '%Condition:%' OR explain ILIKE '%Granules: %/%';
SELECT 'ns in', count() FROM t_ns WHERE time IN (INTERVAL 510 MINUTE, INTERVAL 1 HOUR);
SELECT 'ns in value', time FROM t_ns WHERE time IN (INTERVAL 510 MINUTE, INTERVAL 1 HOUR) ORDER BY time;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ns WHERE time IN (INTERVAL 510 MINUTE, INTERVAL 1 HOUR)) WHERE explain ILIKE '%Granules: %/%';
SELECT 'ns not in', count() FROM t_ns WHERE time NOT IN (INTERVAL 510 MINUTE, INTERVAL 1 HOUR);
SELECT 'ns not in excludes', countIf(time IN (toIntervalNanosecond(3600000000000), toIntervalNanosecond(30600000000000))) FROM t_ns WHERE time NOT IN (INTERVAL 510 MINUTE, INTERVAL 1 HOUR);
SELECT 'ns in array with null', count() FROM t_ns WHERE time IN [NULL, INTERVAL 510 MINUTE];
SELECT 'ns in array with null value', time FROM t_ns WHERE time IN [NULL, INTERVAL 510 MINUTE] ORDER BY time;
SELECT 'ns prewhere', count() FROM t_ns PREWHERE time >= INTERVAL 30600 SECOND AND time < INTERVAL 516 MINUTE;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ns PREWHERE time >= INTERVAL 30600 SECOND AND time < INTERVAL 516 MINUTE) WHERE explain ILIKE '%Condition:%' OR explain ILIKE '%Granules: %/%';
SELECT 'ms between minute', count() FROM t_ms WHERE time BETWEEN INTERVAL 510 MINUTE AND INTERVAL 515 MINUTE;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ms WHERE time BETWEEN INTERVAL 510 MINUTE AND INTERVAL 515 MINUTE) WHERE explain ILIKE '%Condition:%' OR explain ILIKE '%Granules: %/%';

-- `minmax` skip indexes on `IntervalNanosecond`, `IntervalMicrosecond` and `IntervalSecond` columns.
SELECT 'skip ns', count() FROM t_skip WHERE time BETWEEN INTERVAL 510 MINUTE AND INTERVAL 515 MINUTE;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_skip WHERE time BETWEEN INTERVAL 510 MINUTE AND INTERVAL 515 MINUTE) WHERE explain ILIKE '%Condition:%' OR explain ILIKE '%Granules: %/%' OR explain ILIKE '%Name: mm%';
SELECT 'skip us', count() FROM t_skip WHERE us >= INTERVAL 27 HOUR;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_skip WHERE us >= INTERVAL 27 HOUR) WHERE explain ILIKE '%Condition:%' OR explain ILIKE '%Granules: %/%' OR explain ILIKE '%Name: mm%';
SELECT 's in', count() FROM t_skip WHERE s IN (INTERVAL 510 MINUTE, INTERVAL 1 HOUR);
SELECT 's in value', s FROM t_skip WHERE s IN (INTERVAL 510 MINUTE, INTERVAL 1 HOUR) ORDER BY s;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_skip WHERE s IN (INTERVAL 510 MINUTE, INTERVAL 1 HOUR)) WHERE explain ILIKE '%Condition:%' OR explain ILIKE '%Granules: %/%' OR explain ILIKE '%Name: mm%';
SELECT 'us in value', us FROM t_skip WHERE us IN (INTERVAL 510 MINUTE, INTERVAL 1 HOUR) ORDER BY us;

-- `Month`, `Quarter` and `Year`.
SELECT 'month', count() FROM t_month WHERE m >= INTERVAL 2 YEAR AND m < INTERVAL 9 QUARTER;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_month WHERE m >= INTERVAL 2 YEAR AND m < INTERVAL 9 QUARTER) WHERE explain ILIKE '%Condition:%' OR explain ILIKE '%Granules: %/%';
SELECT 'month in', count() FROM t_month WHERE m IN (INTERVAL 1 YEAR, INTERVAL 5 QUARTER);
SELECT 'month in value', m FROM t_month WHERE m IN (INTERVAL 1 YEAR, INTERVAL 5 QUARTER) ORDER BY m;

-- A finer constant, a constant that overflows the unit of the column and a constant of the other family are not converted.
SELECT count() FROM t_ms WHERE time IN (toIntervalMicrosecond(30600000000), toIntervalMicrosecond(1)); -- { serverError TYPE_MISMATCH }
SELECT 'ns overflow', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_ns WHERE time >= INTERVAL 20000 WEEK) WHERE explain ILIKE '%Condition:%';
SELECT count() FROM t_ns WHERE time IN (INTERVAL 20000 WEEK, INTERVAL 1 SECOND); -- { serverError TYPE_MISMATCH }
SELECT count() FROM t_month WHERE m IN (INTERVAL 30 DAY, INTERVAL 1 YEAR); -- { serverError TYPE_MISMATCH }
SELECT count() FROM t_ns WHERE time IN (INTERVAL 1 MONTH, INTERVAL 1 SECOND); -- { serverError TYPE_MISMATCH }

DROP TABLE t_ns;
DROP TABLE t_ms;
DROP TABLE t_skip;
DROP TABLE t_month;
