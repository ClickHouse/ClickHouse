-- Serialized `gini` states must be validated like ordinary input values.

-- Int64 min and max.
SELECT finalizeAggregation(CAST(unhex('020000000000000080FFFFFFFFFFFFFF7F') AS AggregateFunction(gini, Int64))); -- { serverError BAD_ARGUMENTS }
-- Negative value: [-1, 2].
SELECT finalizeAggregation(CAST(unhex('02FFFFFFFFFFFFFFFF0200000000000000') AS AggregateFunction(gini, Int64))); -- { serverError BAD_ARGUMENTS }
-- Infinity: [0, inf].
SELECT finalizeAggregation(CAST(unhex('020000000000000000000000000000F07F') AS AggregateFunction(gini, Float64))); -- { serverError BAD_ARGUMENTS }
-- NaN is never stored by `add`: [0, nan].
SELECT finalizeAggregation(CAST(unhex('020000000000000000000000000000F87F') AS AggregateFunction(gini, Float64))); -- { serverError INCORRECT_DATA }

-- Valid states still round-trip.
SELECT finalizeAggregation(CAST(unhex('0201000000000000000300000000000000') AS AggregateFunction(gini, Int64)));
SELECT round(finalizeAggregation(CAST(unhex(hex(giniState(number))) AS AggregateFunction(gini, UInt64))), 6) FROM numbers(10);
SELECT round(finalizeAggregation(CAST(unhex(hex(giniState(toFloat64(number)))) AS AggregateFunction(gini, Float64))), 6) FROM numbers(10);
