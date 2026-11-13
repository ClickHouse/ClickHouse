-- A deserialized `groupArraySample` state may carry an arbitrary `total_values` counter.
-- Merging must not wrap it around to zero, which led to a division by zero.

-- Valid states merge fine.
SELECT length(arrayReduce('groupArraySampleMerge(1, 1)', [
    CAST(unhex('0101410100000000000000173633363431333632323338343637393330303520302033'), 'AggregateFunction(groupArraySample(1, 1), String)'),
    CAST(unhex('0101420100000000000000173633363431333632323338343637393330303520302033'), 'AggregateFunction(groupArraySample(1, 1), String)')]));
SELECT length(arrayReduce('groupArraySampleMerge(1, 1)', [
    CAST(unhex('01410100000000000000173633363431333632323338343637393330303520302033'), 'AggregateFunction(groupArraySample(1, 1), UInt8)'),
    CAST(unhex('01420100000000000000173633363431333632323338343637393330303520302033'), 'AggregateFunction(groupArraySample(1, 1), UInt8)')]));

-- total_values = UINT64_MAX, merged with a state with one value.
SELECT arrayReduce('groupArraySampleMerge(1, 1)', [
    CAST(unhex('010141FFFFFFFFFFFFFFFF173633363431333632323338343637393330303520302033'), 'AggregateFunction(groupArraySample(1, 1), String)'),
    CAST(unhex('0101420100000000000000173633363431333632323338343637393330303520302033'), 'AggregateFunction(groupArraySample(1, 1), String)')]); -- { serverError INCORRECT_DATA }
SELECT arrayReduce('groupArraySampleMerge(1, 1)', [
    CAST(unhex('0141FFFFFFFFFFFFFFFF173633363431333632323338343637393330303520302033'), 'AggregateFunction(groupArraySample(1, 1), UInt8)'),
    CAST(unhex('01420100000000000000173633363431333632323338343637393330303520302033'), 'AggregateFunction(groupArraySample(1, 1), UInt8)')]); -- { serverError INCORRECT_DATA }

-- Both states have more values than the sample size, and their counters sum to 2^64.
SELECT arrayReduce('groupArraySampleMerge(1, 1)', [
    CAST(unhex('0101410000000000000080173633363431333632323338343637393330303520302033'), 'AggregateFunction(groupArraySample(1, 1), String)'),
    CAST(unhex('0101420000000000000080173633363431333632323338343637393330303520302033'), 'AggregateFunction(groupArraySample(1, 1), String)')]); -- { serverError INCORRECT_DATA }
SELECT arrayReduce('groupArraySampleMerge(1, 1)', [
    CAST(unhex('01410000000000000080173633363431333632323338343637393330303520302033'), 'AggregateFunction(groupArraySample(1, 1), UInt8)'),
    CAST(unhex('01420000000000000080173633363431333632323338343637393330303520302033'), 'AggregateFunction(groupArraySample(1, 1), UInt8)')]); -- { serverError INCORRECT_DATA }

SELECT 'still alive';
