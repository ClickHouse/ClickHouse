-- `estimateCompressionRatio` with a `target_type`: parameters, errors, the common numerator, and the aggregation paths.

-- A state split between threads, replicas or spilled to disk is estimated as several parts.
SET max_threads = 1, enable_parallel_replicas = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

DROP TABLE IF EXISTS src;
CREATE TABLE src (n UInt64, s String) ENGINE = Memory;
INSERT INTO src SELECT number, concat('region_', toString(number % 16)) FROM numbers(50000);
DROP TABLE IF EXISTS variants;
CREATE TABLE variants ENGINE = Memory AS SELECT if(number % 3 = 0, NULL, toString(number))::Variant(String) AS v FROM numbers(100);

SELECT 'parameters';
-- In any order, the codec and the block size optional.
SELECT estimateCompressionRatio('LZ4', 65536, 'LowCardinality(String)')(s) = estimateCompressionRatio('LowCardinality(String)', 65536, 'LZ4')(s) FROM src;
SELECT estimateCompressionRatio('LowCardinality(String)')(s) = estimateCompressionRatio('ZSTD(3)', 'LowCardinality(String)')(s) FROM src;
SELECT estimateCompressionRatio('LZ4', 'String')(s) > 0, estimateCompressionRatio(65536, 'String')(s) > 0 FROM src;
-- No rows.
SELECT estimateCompressionRatio('LZ4', 'String')(s) FROM src WHERE n > 1000000;
-- Composite targets: `Map` has dynamic subcolumns (its keys) but a fixed set of streams.
SELECT
    estimateCompressionRatio('LZ4', 'Map(String, UInt32)')(map('a', n)) > 0,
    estimateCompressionRatio('LZ4', 'Variant(UInt64, String)')(n) > 0,
    estimateCompressionRatio('LZ4', 'Tuple(a UInt32, b String)')(tuple(n, s)) > 0
FROM src;

SELECT 'errors';
SELECT estimateCompressionRatio('String', 'UInt64')(n) FROM src; -- { serverError BAD_QUERY_PARAMETER }
SELECT estimateCompressionRatio('LZ4', 'ZSTD', 'String')(n) FROM src; -- { serverError BAD_QUERY_PARAMETER }
SELECT estimateCompressionRatio('LZ4', 65536, 8192)(n) FROM src; -- { serverError BAD_QUERY_PARAMETER }
SELECT estimateCompressionRatio('LZ4', 0, 'String')(n) FROM src; -- { serverError BAD_QUERY_PARAMETER }
SELECT estimateCompressionRatio('LZ4', 65536, 'String', 1)(n) FROM src; -- { serverError UNKNOWN_QUERY_PARAMETER }
-- A string that starts with the name of a type is a type, with the error of that type.
SELECT estimateCompressionRatio('Decimal(100, 2)')(n) FROM src; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT estimateCompressionRatio('LowCardinality(Strin)')(n) FROM src; -- { serverError UNKNOWN_TYPE }
-- A conversion the types do not allow fails without rows; a value the target cannot hold fails when it comes.
SELECT estimateCompressionRatio('Array(UInt8)')(n) FROM src WHERE n > 1000000; -- { serverError TYPE_MISMATCH }
SELECT estimateCompressionRatio('UInt8')(n) FROM src; -- { serverError CANNOT_CONVERT_TYPE }
SELECT estimateCompressionRatio('UInt8')(n) FROM src WHERE n < 256 FORMAT Null;
SELECT estimateCompressionRatio('String')(nullIf(s, 'region_5')) FROM src; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT estimateCompressionRatio('Nullable(String)')(nullIf(s, 'region_5')) > 0 FROM src;
-- NULL rows of `if` are measured too: the optimizer does not turn `f(if(c, NULL, x))` into `fIf(x, not c)`.
SELECT estimateCompressionRatio('String')(if(n = 5, NULL, s)) FROM src; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT estimateCompressionRatio('Nullable(String)')(if(n % 10 = 0, NULL, s))
    = (SELECT estimateCompressionRatio('Nullable(String)')(if(n % 10 = 0, NULL, s)) FROM src SETTINGS optimize_rewrite_aggregate_function_with_if = 0)
FROM src;
SELECT estimateCompressionRatio('UInt32')(s) FROM src; -- { serverError CANNOT_PARSE_TEXT }
-- The conversion follows the settings of the query, as `accurateCast`.
SELECT estimateCompressionRatio('Enum8(\'only\' = 1)')(toUInt8(2)); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT estimateCompressionRatio('DateTime')('01/02/2020') > 0 SETTINGS cast_string_to_date_time_mode = 'best_effort';
SELECT estimateCompressionRatio('DateTime')('01/02/2020') SETTINGS cast_string_to_date_time_mode = 'basic'; -- { serverError CANNOT_PARSE_DATETIME }
SELECT estimateCompressionRatio('DateTime(\'UTC\')')(toDateTime64('1960-01-01 00:00:00', 3, 'UTC') + toIntervalSecond(number))
FROM numbers(20) SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- The compressed size (the second number of the state) is that of the values `accurateCast` gives.
WITH '^(?:[89A-F][0-9A-F])*[0-7][0-9A-F]((?:[89A-F][0-9A-F])*[0-7][0-9A-F])$' AS second_number
SELECT
    extract(hex(estimateCompressionRatioState('LZ4', 'DateTime(\'UTC\')')(x)), second_number)
    = extract(hex(estimateCompressionRatioState('LZ4', 'DateTime(\'UTC\')')(accurateCast(x, 'DateTime(\'UTC\')'))), second_number)
FROM (SELECT toDateTime64('1960-01-01 00:00:00', 3, 'UTC') + toIntervalSecond(number) AS x FROM numbers(20))
SETTINGS date_time_overflow_behavior = 'ignore';
-- Suspicious and experimental types need their settings, as in `CAST`.
SELECT estimateCompressionRatio('LowCardinality(UInt8)')(toUInt8(n)) FROM src; -- { serverError SUSPICIOUS_TYPE_FOR_LOW_CARDINALITY }
SELECT estimateCompressionRatio('LowCardinality(UInt8)')(toUInt8(n)) > 0 FROM src SETTINGS allow_suspicious_low_cardinality_types = 1;
-- Only rows are checked: a stored state that names such a type is read, merged and attached without the setting.
DROP TABLE IF EXISTS lc_states;
CREATE TABLE lc_states (st AggregateFunction(estimateCompressionRatio('LowCardinality(UInt8)'), UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO lc_states SELECT estimateCompressionRatioState('LowCardinality(UInt8)')(toUInt8(n % 10)) FROM src SETTINGS allow_suspicious_low_cardinality_types = 1;
DETACH TABLE lc_states;
ATTACH TABLE lc_states;
SELECT min(finalizeAggregation(st)) > 0, estimateCompressionRatioMerge('LowCardinality(UInt8)')(st) > 0 FROM lc_states;
DROP TABLE lc_states;
-- A codec the target type cannot take, or that compresses nothing by itself.
SELECT estimateCompressionRatio('T64', 'String')(n) FROM src WHERE n > 1000000; -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }
SELECT estimateCompressionRatio('Delta', 'LowCardinality(String)')(n) FROM src WHERE n > 1000000; -- { serverError BAD_ARGUMENTS }
SELECT estimateCompressionRatio('Quantized(8)', 'Array(Float32)')([1.0]) FROM src; -- { serverError BAD_ARGUMENTS }
-- Types whose substreams depend on the data are not supported as targets.
SELECT estimateCompressionRatio('JSON')(s) FROM src; -- { serverError NOT_IMPLEMENTED }
SELECT estimateCompressionRatio('Tuple(a UInt8, b Dynamic)')(tuple(1, 2)) FROM src; -- { serverError NOT_IMPLEMENTED }
SELECT estimateCompressionRatio('Map(String, JSON)')(map('a', s)) FROM src; -- { serverError NOT_IMPLEMENTED }
-- Nor as arguments: the rows would carry the dynamic structure of the blocks they came in. Converted in the query, they work.
SELECT estimateCompressionRatio('LZ4', 'String')(s::JSON) FROM src; -- { serverError NOT_IMPLEMENTED }
SELECT estimateCompressionRatio('LZ4', 'String')(tuple(n::Dynamic)) FROM src; -- { serverError NOT_IMPLEMENTED }
-- A `LowCardinality` inside a `Variant` (the only place where it reaches the function) writes a dictionary with every piece.
SELECT estimateCompressionRatio('LZ4', 'String')(s::Variant(LowCardinality(String))) FROM src; -- { serverError NOT_IMPLEMENTED }
SELECT estimateCompressionRatio('LZ4', 'String')(toLowCardinality(s)) > 0, estimateCompressionRatio('LZ4', 'Array(String)')([toLowCardinality(s)]) > 0 FROM src;
SELECT estimateCompressionRatio('LZ4', 'String')(toString(toJSONString(map('a', n))::JSON)) > 0 FROM src;

SELECT 'numerator';
-- The first number of the state is the bytes of the rows in their own type: the same for every target and codec, and the same
-- as without a target. A `LowCardinality` column counts as its values.
SELECT uniqExact(extract(h, '^(?:[89A-F][0-9A-F])*[0-7][0-9A-F]')), count() FROM
(
    SELECT hex(estimateCompressionRatioState('NONE')(s)) AS h FROM src
    UNION ALL SELECT hex(estimateCompressionRatioState('LZ4', 'String')(s)) FROM src
    UNION ALL SELECT hex(estimateCompressionRatioState('ZSTD(3)', 'LowCardinality(String)')(s)) FROM src
    UNION ALL SELECT hex(estimateCompressionRatioState('ZSTD(1)', 8192, 'Nullable(String)')(s)) FROM src
    UNION ALL SELECT hex(estimateCompressionRatioState('LZ4', 'String')(toLowCardinality(s))) FROM src
);
-- Also when the targets end their granules at other rows: 100 rows of `FixedString(262144)` are 25 MiB, three granules, while
-- `String` writes one. A `Variant` writes a prefix with each serialization, which must count once.
SELECT uniqExact(extract(h, '^(?:[89A-F][0-9A-F])*[0-7][0-9A-F]')), count() FROM
(
    SELECT hex(estimateCompressionRatioState('LZ4', 'String')(v)) AS h FROM variants
    UNION ALL SELECT hex(estimateCompressionRatioState('LZ4', 'FixedString(262144)')(v)) FROM variants
)
SETTINGS allow_suspicious_fixed_string_types = 1;

SELECT 'ranking';
-- 16 distinct values: much smaller as `LowCardinality`, with either codec.
SELECT
    estimateCompressionRatio('ZSTD(3)', 'LowCardinality(String)')(s) > 10 * estimateCompressionRatio('ZSTD(3)', 'String')(s),
    estimateCompressionRatio('LZ4', 'LowCardinality(String)')(s) > 10 * estimateCompressionRatio('ZSTD(3)', 'String')(s)
FROM src;
-- Unique values: smaller as `String`.
SELECT estimateCompressionRatio('LZ4', 'String')(x) > estimateCompressionRatio('LZ4', 'LowCardinality(String)')(x)
FROM (SELECT toString(cityHash64(n)) AS x FROM src);
-- Small numbers: smaller in a narrower type.
SELECT estimateCompressionRatio('LZ4', 'UInt16')(n % 1000) > estimateCompressionRatio('LZ4', 'UInt64')(n % 1000) FROM src;

SELECT 'block size';
-- Rows are written in granules of 8192, whatever blocks they come in.
DROP TABLE IF EXISTS results;
CREATE TABLE results (r Float64) ENGINE = Memory;
INSERT INTO results SELECT estimateCompressionRatio('ZSTD(3)', 'LowCardinality(String)')(s) FROM src SETTINGS max_block_size = 1000;
INSERT INTO results SELECT estimateCompressionRatio('ZSTD(3)', 'LowCardinality(String)')(s) FROM src SETTINGS max_block_size = 8192;
INSERT INTO results SELECT estimateCompressionRatio('ZSTD(3)', 'LowCardinality(String)')(s) FROM src SETTINGS max_block_size = 65409;
SELECT uniqExact(r), count() FROM results;
-- Also with rows of varying width that make a granule wider than a state holds, so that it is written in pieces.
TRUNCATE TABLE results;
INSERT INTO results SELECT estimateCompressionRatio('LZ4', 'LowCardinality(String)')(if(intDiv(number, 3000) % 2 = 0, repeat('a', 10 + number % 7), repeat(toString(number % 97), 40 + number % 300))) FROM numbers(20000) SETTINGS max_block_size = 1000;
INSERT INTO results SELECT estimateCompressionRatio('LZ4', 'LowCardinality(String)')(if(intDiv(number, 3000) % 2 = 0, repeat('a', 10 + number % 7), repeat(toString(number % 97), 40 + number % 300))) FROM numbers(20000) SETTINGS max_block_size = 8192;
INSERT INTO results SELECT estimateCompressionRatio('LZ4', 'LowCardinality(String)')(if(intDiv(number, 3000) % 2 = 0, repeat('a', 10 + number % 7), repeat(toString(number % 97), 40 + number % 300))) FROM numbers(20000) SETTINGS max_block_size = 65409;
INSERT INTO results SELECT estimateCompressionRatio('LZ4', 'LowCardinality(String)')(if(intDiv(number, 3000) % 2 = 0, repeat('a', 10 + number % 7), repeat(toString(number % 97), 40 + number % 300))) FROM numbers(20000) GROUP BY number % 1;
SELECT uniqExact(r), count() FROM results;
-- And with rows of 1.5 to 3 KB, whose granules end at `index_granularity_bytes` (10 MiB) before 8192 rows.
TRUNCATE TABLE results;
INSERT INTO results SELECT estimateCompressionRatio('ZSTD(1)', 'String')(repeat(toString(number % 89), 1500 + number % 1500)) FROM numbers(15000) SETTINGS max_block_size = 1000;
INSERT INTO results SELECT estimateCompressionRatio('ZSTD(1)', 'String')(repeat(toString(number % 89), 1500 + number % 1500)) FROM numbers(15000) SETTINGS max_block_size = 8192;
INSERT INTO results SELECT estimateCompressionRatio('ZSTD(1)', 'String')(repeat(toString(number % 89), 1500 + number % 1500)) FROM numbers(15000) SETTINGS max_block_size = 65409;
INSERT INTO results SELECT estimateCompressionRatio('ZSTD(1)', 'String')(repeat(toString(number % 89), 1500 + number % 1500)) FROM numbers(15000) GROUP BY number % 1;
SELECT uniqExact(r), count() FROM results;

SELECT 'aggregation';
-- A group is estimated as the same rows without keys.
SELECT groupArray(r) = [
        (SELECT estimateCompressionRatio('LZ4', 'LowCardinality(String)')(s) FROM src WHERE n % 3 = 0),
        (SELECT estimateCompressionRatio('LZ4', 'LowCardinality(String)')(s) FROM src WHERE n % 3 = 1),
        (SELECT estimateCompressionRatio('LZ4', 'LowCardinality(String)')(s) FROM src WHERE n % 3 = 2)]
FROM (SELECT n % 3 AS k, estimateCompressionRatio('LZ4', 'LowCardinality(String)')(s) AS r FROM src GROUP BY k ORDER BY k);
-- `-If` without keys and with keys.
SELECT estimateCompressionRatioIf('LZ4', 'String')(s, n % 2 = 0) = (SELECT estimateCompressionRatio('LZ4', 'String')(s) FROM src WHERE n % 2 = 0)
FROM src;
SELECT estimateCompressionRatioIf('LZ4', 'String')(s, n % 2 = 0) = (SELECT estimateCompressionRatio('LZ4', 'String')(s) FROM src WHERE n % 2 = 0)
FROM src GROUP BY n % 1;
-- A window over the whole partition gives the result without a window.
SELECT DISTINCT estimateCompressionRatio('LZ4', 'String')(s) OVER () = (SELECT estimateCompressionRatio('LZ4', 'String')(s) FROM src) FROM src;
-- A growing frame is estimated step by step, as without a target.
SELECT count(), min(r) > 0 FROM
(
    SELECT estimateCompressionRatio('LZ4', 'String')(s) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r
    FROM src WHERE n < 1000
);

SELECT 'states';
-- `-State` keeps the two sizes; `-Merge` of one state and `finalizeAggregation` give the result.
SELECT
    finalizeAggregation(estimateCompressionRatioState('LZ4', 'String')(s)) = estimateCompressionRatio('LZ4', 'String')(s),
    toTypeName(estimateCompressionRatioState('LZ4', 'String')(s))
FROM src;
SELECT estimateCompressionRatioMerge('LZ4', 'String')(st) = (SELECT estimateCompressionRatio('LZ4', 'String')(s) FROM src)
FROM (SELECT estimateCompressionRatioState('LZ4', 'String')(s) AS st FROM src);
-- Merged states add up their sizes, in a query and in a table.
DROP TABLE IF EXISTS states;
CREATE TABLE states (k UInt8, st AggregateFunction(estimateCompressionRatio('LZ4', 'String'), String)) ENGINE = MergeTree ORDER BY k;
INSERT INTO states SELECT 0, estimateCompressionRatioState('LZ4', 'String')(s) FROM src WHERE n < 25000;
INSERT INTO states SELECT 0, estimateCompressionRatioState('LZ4', 'String')(s) FROM src WHERE n >= 25000;
SELECT estimateCompressionRatioMerge('LZ4', 'String')(st) = (
    SELECT estimateCompressionRatioMerge('LZ4', 'String')(st) FROM
    (
        SELECT estimateCompressionRatioState('LZ4', 'String')(s) AS st FROM src WHERE n < 25000
        UNION ALL SELECT estimateCompressionRatioState('LZ4', 'String')(s) FROM src WHERE n >= 25000
    ))
FROM states;

-- A state kept in a `Memory` table is read by later queries, also at once: the first read finishes it, the others see the
-- same sizes.
DROP TABLE IF EXISTS memory_states;
CREATE TABLE memory_states (st AggregateFunction(estimateCompressionRatio('LZ4', 'String'), String)) ENGINE = Memory;
INSERT INTO memory_states SELECT estimateCompressionRatioState('LZ4', 'String')(s) FROM src;
SELECT finalizeAggregation(st) = (SELECT estimateCompressionRatio('LZ4', 'String')(s) FROM src) FROM memory_states;
SELECT finalizeAggregation(st) = (SELECT estimateCompressionRatio('LZ4', 'String')(s) FROM src) FROM memory_states;
DROP TABLE memory_states;

-- Its pending rows are converted with the settings of the query that added them, whoever reads it first: two states added
-- with different overflow settings, merged before anything finalizes them by a query whose setting would throw.
DROP TABLE IF EXISTS overflow_states;
DROP TABLE IF EXISTS overflow_results;
CREATE TABLE overflow_states (k UInt8, st AggregateFunction(estimateCompressionRatio('LZ4', 'DateTime(\'UTC\')'), DateTime64(3, 'UTC'))) ENGINE = Memory;
CREATE TABLE overflow_results (kind String, k UInt8, r Float64) ENGINE = Memory;
INSERT INTO overflow_states SELECT 0, estimateCompressionRatioState('LZ4', 'DateTime(\'UTC\')')(toDateTime64('1960-01-01 00:00:00', 3, 'UTC') + toIntervalSecond(number))
FROM numbers(20) SETTINGS date_time_overflow_behavior = 'ignore';
INSERT INTO overflow_states SELECT 1, estimateCompressionRatioState('LZ4', 'DateTime(\'UTC\')')(toDateTime64('1960-01-01 00:00:00', 3, 'UTC') + toIntervalSecond(number))
FROM numbers(20) SETTINGS date_time_overflow_behavior = 'saturate';
INSERT INTO overflow_results SELECT 'merged', k, estimateCompressionRatioMerge('LZ4', 'DateTime(\'UTC\')')(st) FROM overflow_states GROUP BY k
SETTINGS date_time_overflow_behavior = 'throw';
INSERT INTO overflow_results SELECT 'direct', 0, estimateCompressionRatio('LZ4', 'DateTime(\'UTC\')')(toDateTime64('1960-01-01 00:00:00', 3, 'UTC') + toIntervalSecond(number))
FROM numbers(20) SETTINGS date_time_overflow_behavior = 'ignore';
INSERT INTO overflow_results SELECT 'direct', 1, estimateCompressionRatio('LZ4', 'DateTime(\'UTC\')')(toDateTime64('1960-01-01 00:00:00', 3, 'UTC') + toIntervalSecond(number))
FROM numbers(20) SETTINGS date_time_overflow_behavior = 'saturate';
INSERT INTO overflow_results SELECT 'finalized', k, finalizeAggregation(st) FROM overflow_states SETTINGS date_time_overflow_behavior = 'throw';
-- Each state as its own query would have estimated it (the two differ), and unchanged by the merge.
SELECT k, uniqExact(r), count() FROM overflow_results GROUP BY k ORDER BY k;
SELECT uniqExact(r) FROM overflow_results;
DROP TABLE overflow_states;
DROP TABLE overflow_results;
-- And with the time zone of that query: a `DateTime` without one takes the session's. A state added under UTC, merged first
-- by a query under another time zone, gives what UTC gives.
DROP TABLE IF EXISTS tz_states;
DROP TABLE IF EXISTS tz_results;
CREATE TABLE tz_states (st AggregateFunction(estimateCompressionRatio('ZSTD(3)', 'DateTime'), String)) ENGINE = Memory;
CREATE TABLE tz_results (kind String, r Float64) ENGINE = Memory;
INSERT INTO tz_states SELECT estimateCompressionRatioState('ZSTD(3)', 'DateTime')(toString(toDateTime('1970-01-01 01:00:00', 'UTC') + number))
FROM numbers(1000) SETTINGS session_timezone = 'UTC';
INSERT INTO tz_results SELECT 'merged', estimateCompressionRatioMerge('ZSTD(3)', 'DateTime')(st) FROM tz_states SETTINGS session_timezone = 'Asia/Tokyo';
INSERT INTO tz_results SELECT 'direct', estimateCompressionRatio('ZSTD(3)', 'DateTime')(toString(toDateTime('1970-01-01 01:00:00', 'UTC') + number))
FROM numbers(1000) SETTINGS session_timezone = 'UTC';
SELECT uniqExact(r), count() FROM tz_results;
DROP TABLE tz_states;
DROP TABLE tz_results;
-- The validation of the target type is the one of the query that added the rows, too.
DROP TABLE IF EXISTS lc_memory_states;
CREATE TABLE lc_memory_states (st AggregateFunction(estimateCompressionRatio('LowCardinality(UInt8)'), UInt8)) ENGINE = Memory;
INSERT INTO lc_memory_states SELECT estimateCompressionRatioState('LowCardinality(UInt8)')(toUInt8(n % 10)) FROM src WHERE n < 1000
SETTINGS allow_suspicious_low_cardinality_types = 1;
SELECT estimateCompressionRatioMerge('LowCardinality(UInt8)')(st) > 0 FROM lc_memory_states;
DROP TABLE lc_memory_states;

SELECT 'sparse';
-- A column read in sparse form is estimated as the same rows read dense.
DROP TABLE IF EXISTS sparse;
CREATE TABLE sparse (n UInt64, v UInt64) ENGINE = MergeTree ORDER BY n
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, serialization_info_version = 'with_types';
INSERT INTO sparse SELECT number, if(number % 20 = 0, number, 0) FROM numbers(50000);
SELECT serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'sparse' AND column = 'v' AND active;
SELECT estimateCompressionRatio('LZ4', 'UInt32')(v) = (SELECT estimateCompressionRatio('LZ4', 'UInt32')(if(number % 20 = 0, number, 0)) FROM numbers(50000))
FROM sparse;
SELECT estimateCompressionRatio('LZ4', 'UInt32')(v) = (SELECT estimateCompressionRatio('LZ4', 'UInt32')(if(number % 20 = 0, number, 0)) FROM numbers(50000))
FROM sparse GROUP BY n % 1;

DROP TABLE src;
DROP TABLE variants;
DROP TABLE results;
DROP TABLE states;
DROP TABLE sparse;
