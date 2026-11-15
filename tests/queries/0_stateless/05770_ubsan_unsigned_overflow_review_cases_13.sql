-- Aggregate states read from their serialized form.

-- A 'large' `quantileTiming` state with one bin of `5`, and one with a count but no bins: the values at or above
-- `BIG_THRESHOLD` (30000) are counted without a bin, so it is valid, and its quantile is 30000.
SELECT finalizeAggregation(CAST(unhex('030100000000000000050001000000000000003075'), 'AggregateFunction(quantileTiming(0.5), UInt64)'));
SELECT finalizeAggregation(CAST(unhex('0305000000000000003075'), 'AggregateFunction(quantileTiming(0.5), UInt64)'));
SELECT finalizeAggregation(CAST(unhex('0320000000000000003075'), 'AggregateFunction(quantileTimingWeighted, UInt64, UInt64)'));
-- Serialized states with values below and above the threshold round-trip.
SELECT quantilesTimingMerge(0.1, 0.4, 0.9)(CAST(unhex(hex(s)), 'AggregateFunction(quantilesTiming(0.1, 0.4, 0.9), UInt64)'))
FROM (SELECT quantilesTimingState(0.1, 0.4, 0.9)(number * 7) AS s FROM numbers(10000) GROUP BY number % 3)
SETTINGS max_threads = 1;

-- A `groupConcat` state that claims 2^40 rows without any offsets fails on the missing data, without allocating for them.
SELECT finalizeAggregation(CAST(unhex('03302C310200010203'), 'AggregateFunction(groupConcat(\',\', 2), String)'));
SELECT finalizeAggregation(CAST(unhex('00808080808020'), 'AggregateFunction(groupConcat(\',\', 1), String)'))
SETTINGS max_memory_usage = 1000000000; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
