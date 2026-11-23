-- Tags: no-fasttest
-- ^^ ANTLR4 support is disabled in the fast-test build, and the PromQL grammar requires it.

-- The time arguments of `prometheusQuery`, `prometheusQueryRange` and `timeSeriesSelector` are constant expressions,
-- read by their exact data type: integers, floats, `DateTime`, `DateTime64`, `Interval` (a duration), strings, also
-- under `Nullable` or `LowCardinality`. `Bool` (a literal or an expression) is not a time and NULL has no value, so
-- both are rejected.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_time_arguments;
CREATE TABLE ts_time_arguments (samples Array(Tuple(DateTime64(3, 'UTC'), Float64))) ENGINE = TimeSeries;

SELECT '-- timestamps';
SELECT timestamp, value FROM prometheusQuery('ts_time_arguments', '7', 1000);
SELECT timestamp, value FROM prometheusQuery('ts_time_arguments', '7', toInt32(1000));
SELECT timestamp, value FROM prometheusQuery('ts_time_arguments', '7', toFloat32(1000.5));
SELECT timestamp, value FROM prometheusQuery('ts_time_arguments', '7', 1000.25);
SELECT timestamp, value FROM prometheusQuery('ts_time_arguments', '7', toDateTime('2024-01-01 00:00:00', 'UTC'));
SELECT timestamp, value FROM prometheusQuery('ts_time_arguments', '7', toDateTime64('2024-01-01 00:00:00.123', 3, 'UTC'));
SELECT timestamp, value FROM prometheusQuery('ts_time_arguments', '7', toDecimal64(1000.5, 1));
SELECT timestamp, value FROM prometheusQuery('ts_time_arguments', '7', '2024-01-01T00:00:00.5Z');
SELECT timestamp, value FROM prometheusQuery('ts_time_arguments', '7', toNullable(1000));
SELECT timestamp, value FROM prometheusQuery('ts_time_arguments', '7', toLowCardinality('1000'));
SELECT timestamp, value FROM prometheusQuery('ts_time_arguments', '7', toFixedString('1000', 4));
-- `Variant` and `Dynamic` hold the value of their active alternative.
SELECT timestamp, value FROM prometheusQuery('ts_time_arguments', '7', CAST(toUInt64(1000) AS Variant(UInt64, String)));
SELECT timestamp, value FROM prometheusQuery('ts_time_arguments', '7', CAST('2024-01-01T00:00:00Z' AS Variant(UInt64, String)));
SELECT timestamp, value FROM prometheusQuery('ts_time_arguments', '7', CAST(1000 AS Dynamic));
-- A comparison is a `UInt8`, not a `Bool`.
SELECT timestamp, value FROM prometheusQuery('ts_time_arguments', '7', 1 = 1);

SELECT '-- durations';
SELECT * FROM prometheusQueryRange('ts_time_arguments', '7', 1000, 1060, INTERVAL 30 SECOND);
SELECT * FROM prometheusQueryRange('ts_time_arguments', '7', 1000, 1001, INTERVAL 500 MILLISECOND);
SELECT * FROM prometheusQueryRange('ts_time_arguments', '7', 1000, 1060, '30s');
SELECT * FROM prometheusQueryRange('ts_time_arguments', '7', 1000, 1060, toFloat64(30));
SELECT * FROM prometheusQueryRange('ts_time_arguments', '7', CAST(1000 AS Dynamic), 1060, CAST(toFloat64(30) AS Dynamic));

SELECT '-- timeSeriesSelector';
SELECT count() FROM timeSeriesSelector('ts_time_arguments', 'up', toDateTime64('2024-01-01 00:00:00', 3, 'UTC'), toDateTime64('2024-01-02 00:00:00', 3, 'UTC'), 'samples');
SELECT count() FROM timeSeriesSelector('ts_time_arguments', 'up', 1000, toFloat32(2000), 'samples');

SELECT '-- rejected';
SELECT timestamp FROM prometheusQuery('ts_time_arguments', '7', true); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts_time_arguments', '7', toBool(1)); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts_time_arguments', '7', CAST(true AS Variant(Bool, String))); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts_time_arguments', '7', CAST(NULL AS Dynamic)); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts_time_arguments', '7', CAST(NULL AS Nullable(UInt32))); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts_time_arguments', '7', nan); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQuery('ts_time_arguments', '7', toUUID('00000000-0000-0000-0000-000000000000')); -- { serverError BAD_ARGUMENTS }
SELECT timestamp FROM prometheusQueryRange('ts_time_arguments', '7', 1000, 1060, true); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM timeSeriesSelector('ts_time_arguments', 'up', 1000, false, 'samples'); -- { serverError BAD_ARGUMENTS }

DROP TABLE ts_time_arguments;
