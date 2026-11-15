-- Malformed aggregate states are rejected instead of finalizing to a wrong value or allocating for their claims.

-- A 'large' `quantileTiming` state whose bins do not sum to its count. A valid one with one bin of `5` still reads.
SELECT finalizeAggregation(CAST(unhex('030100000000000000050001000000000000003075'), 'AggregateFunction(quantileTiming(0.5), UInt64)'));
SELECT finalizeAggregation(CAST(unhex('0305000000000000003075'), 'AggregateFunction(quantileTiming(0.5), UInt64)')); -- { serverError INCORRECT_DATA }

-- A `groupConcat` state that claims 2^40 rows without any offsets fails on the missing data, without allocating for them.
SELECT finalizeAggregation(CAST(unhex('03302C310200010203'), 'AggregateFunction(groupConcat(\',\', 2), String)'));
SELECT finalizeAggregation(CAST(unhex('00808080808020'), 'AggregateFunction(groupConcat(\',\', 1), String)'))
SETTINGS max_memory_usage = 1000000000; -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
