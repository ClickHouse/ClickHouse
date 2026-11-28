-- A merged count is the modular `UInt64` sum of the partial counts: multiplying a state merges it repeatedly.
SELECT finalizeAggregation(multiply(s, toUInt64(9223372036854775808))) FROM (SELECT countState() AS s FROM numbers(2));
SELECT finalizeAggregation(multiply(s, toUInt64(9223372036854775808))) FROM (SELECT countState(toNullable(number)) AS s FROM numbers(2));
SELECT finalizeAggregation(multiply(s, toUInt64(9223372036854775808))) FROM (SELECT regr_countState(toUInt8(number), toUInt8(number)) AS s FROM numbers(2));
SELECT finalizeAggregation(multiply(s, toUInt64(9223372036854775809))) FROM (SELECT countState() AS s FROM numbers(3));
