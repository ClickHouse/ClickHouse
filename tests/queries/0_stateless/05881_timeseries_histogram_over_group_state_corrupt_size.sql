-- A serialized `timeSeriesHistogramSumOverGroup`/`timeSeriesHistogramAvgOverGroup` state with a corrupt array
-- count fails at the end of the data without allocating memory for the claimed number of elements.

SET allow_experimental_time_series_aggregate_functions = 1;
SET enable_nullable_tuple_type = 1;

DROP TABLE IF EXISTS corrupt_states;
CREATE TABLE corrupt_states (name String, state String) ENGINE = Memory;

-- One sample with one positive span and two bucket values. The count 2^32 replaces the span count of the sum
-- state (byte 42, right after the scalars) and the bucket value count of the avg state (byte 67, after the span).
INSERT INTO corrupt_states
SELECT name, overlay(state, unhex('0000000001000000'), position, 8)
FROM
(
    SELECT
        unhex(hex(timeSeriesHistogramSumOverGroupState(payload))) AS sum_state,
        unhex(hex(timeSeriesHistogramAvgOverGroupState(payload))) AS avg_state
    FROM
    (
        SELECT CAST((1, 0, 0., 4., 10., 0., [(0, 2)], [1., 3.], [], [], [], 0, 0, [], []),
            'Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64), count_int UInt64, zero_count_int UInt64, positive_values_int Array(UInt64), negative_values_int Array(UInt64))') AS payload
    )
)
ARRAY JOIN
    ['sum spans', 'avg values'] AS name,
    [sum_state, avg_state] AS state,
    [42, 67] AS position;

SELECT finalizeAggregation(CAST(state, 'AggregateFunction(timeSeriesHistogramSumOverGroup, Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64), count_int UInt64, zero_count_int UInt64, positive_values_int Array(UInt64), negative_values_int Array(UInt64)))'))
FROM corrupt_states WHERE name = 'sum spans' SETTINGS log_comment = '05881 sum spans'; -- { serverError CANNOT_READ_ALL_DATA }

SELECT finalizeAggregation(CAST(state, 'AggregateFunction(timeSeriesHistogramAvgOverGroup, Tuple(flags UInt8, schema Int8, zero_threshold Float64, count Float64, sum Float64, zero_count Float64, positive_spans Array(Tuple(offset Int32, length UInt32)), positive_values Array(Float64), negative_spans Array(Tuple(offset Int32, length UInt32)), negative_values Array(Float64), custom_values Array(Float64), count_int UInt64, zero_count_int UInt64, positive_values_int Array(UInt64), negative_values_int Array(UInt64)))'))
FROM corrupt_states WHERE name = 'avg values' SETTINGS log_comment = '05881 avg values'; -- { serverError CANNOT_READ_ALL_DATA }

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, type, memory_usage < 100000000
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND log_comment LIKE '05881 %' AND type != 'QueryStart'
ORDER BY log_comment;

DROP TABLE corrupt_states;
