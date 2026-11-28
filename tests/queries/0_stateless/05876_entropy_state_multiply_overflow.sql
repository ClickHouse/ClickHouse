-- Multiplying an aggregate state merges it into itself, so the counts of `entropy` reach the UInt64 limit.
SELECT finalizeAggregation(multiply(s, toUInt64(9223372036854775808))) FROM (SELECT entropyState(number) AS s FROM numbers(2));
SELECT finalizeAggregation(multiply(s, toUInt64(4611686018427387904))) FROM (SELECT entropyState(toString(number)) AS s FROM numbers(4));
SELECT finalizeAggregation(multiply(s, toUInt64(4611686018427387904))) FROM (SELECT entropyState(number % 3) AS s FROM numbers(4));
SELECT finalizeAggregation(multiply(s, toUInt64(4))) FROM (SELECT entropyState(number % 3) AS s FROM numbers(4));
SELECT finalizeAggregation(multiply(s, toUInt64(9223372036854775808))) FROM (SELECT entropyState(number % 2) AS s FROM numbers(4)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT round(finalizeAggregation(multiply(a, toUInt64(9007199254740992)) + b) * 1e14, 4) FROM (SELECT entropyStateIf(number, number = 0) AS a, entropyStateIf(number, number > 0) AS b FROM numbers(3));
