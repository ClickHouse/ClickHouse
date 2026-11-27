SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t
(
    timestamps Array(DateTime64(3, 'UTC')),
    values Array(Float64),
    condition UInt8
)
ENGINE = Memory;

INSERT INTO t VALUES ([toDateTime64(100, 3, 'UTC')], [7.0], 1), ([toDateTime64(200, 3, 'UTC')], [9.0], 2), ([toDateTime64(300, 3, 'UTC')], [11.0], 0);

SELECT timeSeriesGroupArrayIf(timestamps, values, condition) FROM t;
SELECT timeSeriesGroupArrayIf(arrayZip(timestamps, values), condition) FROM t;
SELECT timeSeriesGroupArrayIf(timestamps[1], values[1], condition) FROM t;
SELECT timeSeriesGroupArrayIf(timestamps, values, condition != 0) FROM t;

DROP TABLE t;
