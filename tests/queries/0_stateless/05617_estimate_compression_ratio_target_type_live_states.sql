-- `estimateCompressionRatio` with a `target_type`: a state kept in a `Memory` table converts its rows with the settings of the
-- query that added them, whichever query merges or reads it, and however often.

-- A state split between threads or spilled to disk is estimated as several parts.
SET max_threads = 1, enable_parallel_replicas = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

DROP TABLE IF EXISTS states;
DROP TABLE IF EXISTS results;
DROP TABLE IF EXISTS mixed;
DROP TABLE IF EXISTS tz_states;
DROP TABLE IF EXISTS tz_results;
DROP TABLE IF EXISTS lc_states;

-- Values before 1970 overflow `DateTime`: `ignore` wraps them, `saturate` clamps them, `throw` rejects them. A state of 100 rows
-- keeps all of them pending; a state of 10000 rows writes a granule and keeps 1808 pending.
CREATE TABLE states (n UInt32, behavior String, st AggregateFunction(estimateCompressionRatio('LZ4', 'DateTime(\'UTC\')'), DateTime64(3, 'UTC'))) ENGINE = Memory;
CREATE TABLE results (kind String, n UInt32, behavior String, r Float64) ENGINE = Memory;
CREATE TABLE mixed (n UInt32, reader String, r Float64) ENGINE = Memory;

INSERT INTO states SELECT 100, 'ignore', estimateCompressionRatioState('LZ4', 'DateTime(\'UTC\')')(toDateTime64('1960-01-01 00:00:00', 3, 'UTC') + toIntervalSecond(number))
FROM numbers(100) SETTINGS date_time_overflow_behavior = 'ignore';
INSERT INTO states SELECT 100, 'saturate', estimateCompressionRatioState('LZ4', 'DateTime(\'UTC\')')(toDateTime64('1960-01-01 00:00:00', 3, 'UTC') + toIntervalSecond(number))
FROM numbers(100) SETTINGS date_time_overflow_behavior = 'saturate';
INSERT INTO states SELECT 10000, 'ignore', estimateCompressionRatioState('LZ4', 'DateTime(\'UTC\')')(toDateTime64('1960-01-01 00:00:00', 3, 'UTC') + toIntervalSecond(number))
FROM numbers(10000) SETTINGS date_time_overflow_behavior = 'ignore';
INSERT INTO states SELECT 10000, 'saturate', estimateCompressionRatioState('LZ4', 'DateTime(\'UTC\')')(toDateTime64('1960-01-01 00:00:00', 3, 'UTC') + toIntervalSecond(number))
FROM numbers(10000) SETTINGS date_time_overflow_behavior = 'saturate';

INSERT INTO results SELECT 'direct', 100, 'ignore', estimateCompressionRatio('LZ4', 'DateTime(\'UTC\')')(toDateTime64('1960-01-01 00:00:00', 3, 'UTC') + toIntervalSecond(number))
FROM numbers(100) SETTINGS date_time_overflow_behavior = 'ignore';
INSERT INTO results SELECT 'direct', 100, 'saturate', estimateCompressionRatio('LZ4', 'DateTime(\'UTC\')')(toDateTime64('1960-01-01 00:00:00', 3, 'UTC') + toIntervalSecond(number))
FROM numbers(100) SETTINGS date_time_overflow_behavior = 'saturate';
INSERT INTO results SELECT 'direct', 10000, 'ignore', estimateCompressionRatio('LZ4', 'DateTime(\'UTC\')')(toDateTime64('1960-01-01 00:00:00', 3, 'UTC') + toIntervalSecond(number))
FROM numbers(10000) SETTINGS date_time_overflow_behavior = 'ignore';
INSERT INTO results SELECT 'direct', 10000, 'saturate', estimateCompressionRatio('LZ4', 'DateTime(\'UTC\')')(toDateTime64('1960-01-01 00:00:00', 3, 'UTC') + toIntervalSecond(number))
FROM numbers(10000) SETTINGS date_time_overflow_behavior = 'saturate';

-- The first read merges the states added with different settings together, under a setting that rejects the values. The
-- pending rows of one state are written before the rows of the other join them, so each keeps its own conversion.
INSERT INTO mixed SELECT n, 'throw', estimateCompressionRatioMerge('LZ4', 'DateTime(\'UTC\')')(st) FROM states GROUP BY n
SETTINGS date_time_overflow_behavior = 'throw';
INSERT INTO mixed SELECT 0, 'throw', estimateCompressionRatioMerge('LZ4', 'DateTime(\'UTC\')')(st) FROM states
SETTINGS date_time_overflow_behavior = 'throw';

-- Then each state alone, read again and again under other settings.
INSERT INTO results SELECT 'merged', n, behavior, estimateCompressionRatioMerge('LZ4', 'DateTime(\'UTC\')')(st) FROM states GROUP BY n, behavior
SETTINGS date_time_overflow_behavior = 'throw';
INSERT INTO results SELECT 'finalized', n, behavior, finalizeAggregation(st) FROM states
SETTINGS date_time_overflow_behavior = 'throw';
INSERT INTO results SELECT 'merged again', n, behavior, estimateCompressionRatioMerge('LZ4', 'DateTime(\'UTC\')')(st) FROM states GROUP BY n, behavior
SETTINGS date_time_overflow_behavior = 'saturate';

-- Each state gives what the query that added its rows gives, every time.
SELECT n, behavior, uniqExact(r), count() FROM results GROUP BY n, behavior ORDER BY n, behavior;
-- The two settings give different results, so the check above tells them apart.
SELECT n, uniqExact(r) FROM results GROUP BY n ORDER BY n;

-- The merged states give the same result whatever the settings of the query that merges them.
INSERT INTO mixed SELECT n, 'ignore', estimateCompressionRatioMerge('LZ4', 'DateTime(\'UTC\')')(st) FROM states GROUP BY n
SETTINGS date_time_overflow_behavior = 'ignore';
INSERT INTO mixed SELECT 0, 'saturate', estimateCompressionRatioMerge('LZ4', 'DateTime(\'UTC\')')(st) FROM states
SETTINGS date_time_overflow_behavior = 'saturate';
SELECT n, uniqExact(r), count() FROM mixed GROUP BY n ORDER BY n;

-- A `DateTime` without a time zone takes the one of the session that added the rows.
CREATE TABLE tz_states (st AggregateFunction(estimateCompressionRatio('ZSTD(3)', 'DateTime'), String)) ENGINE = Memory;
CREATE TABLE tz_results (kind String, r Float64) ENGINE = Memory;
INSERT INTO tz_states SELECT estimateCompressionRatioState('ZSTD(3)', 'DateTime')(toString(toDateTime('1970-01-01 01:00:00', 'UTC') + number))
FROM numbers(1000) SETTINGS session_timezone = 'UTC';
INSERT INTO tz_results SELECT 'merged', estimateCompressionRatioMerge('ZSTD(3)', 'DateTime')(st) FROM tz_states
SETTINGS session_timezone = 'Asia/Tokyo';
INSERT INTO tz_results SELECT 'direct', estimateCompressionRatio('ZSTD(3)', 'DateTime')(toString(toDateTime('1970-01-01 01:00:00', 'UTC') + number))
FROM numbers(1000) SETTINGS session_timezone = 'UTC';
INSERT INTO tz_results SELECT 'direct in the reader''s time zone', estimateCompressionRatio('ZSTD(3)', 'DateTime')(toString(toDateTime('1970-01-01 01:00:00', 'UTC') + number))
FROM numbers(1000) SETTINGS session_timezone = 'Asia/Tokyo';
SELECT uniqExactIf(r, kind IN ('merged', 'direct')), uniqExact(r) FROM tz_results;

-- The target type is validated with the settings of the query that added the rows: a small state and a partially written
-- one, added with `allow_suspicious_low_cardinality_types`, are merged by a query without it.
CREATE TABLE lc_states (n UInt32, st AggregateFunction(estimateCompressionRatio('LowCardinality(UInt8)'), UInt8)) ENGINE = Memory;
INSERT INTO lc_states SELECT 100, estimateCompressionRatioState('LowCardinality(UInt8)')(toUInt8(number % 10)) FROM numbers(100)
SETTINGS allow_suspicious_low_cardinality_types = 1;
INSERT INTO lc_states SELECT 10000, estimateCompressionRatioState('LowCardinality(UInt8)')(toUInt8(number % 10)) FROM numbers(10000)
SETTINGS allow_suspicious_low_cardinality_types = 1;
SELECT n, estimateCompressionRatioMerge('LowCardinality(UInt8)')(st) > 0 FROM lc_states GROUP BY n ORDER BY n
SETTINGS allow_suspicious_low_cardinality_types = 0;

DROP TABLE states;
DROP TABLE results;
DROP TABLE mixed;
DROP TABLE tz_states;
DROP TABLE tz_results;
DROP TABLE lc_states;
