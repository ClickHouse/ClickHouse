-- Merging two states multiplies their row counts: 2^32 * 2^32 must not wrap around.
SELECT finalizeAggregation(multiply(a, 4294967296) + multiply(b, 4294967296)) FROM (SELECT varPopStableState(toFloat64(0)) AS a, varPopStableState(toFloat64(1)) AS b);
SELECT finalizeAggregation(multiply(a, 4294967296) + multiply(b, 4294967296)) FROM (SELECT covarPopStableState(toFloat64(0), toFloat64(0)) AS a, covarPopStableState(toFloat64(1), toFloat64(1)) AS b);
SELECT finalizeAggregation(multiply(a, 4294967296) + multiply(b, 4294967296)) FROM (SELECT corrStableState(toFloat64(0), toFloat64(0)) AS a, corrStableState(toFloat64(1), toFloat64(1)) AS b);
-- The merged count is the modular `UInt64` sum of the partial counts.
SELECT finalizeAggregation(multiply(s, toUInt64(9223372036854775808))) FROM (SELECT varSampStableState(toNullable(number)) AS s FROM numbers(2));
SELECT finalizeAggregation(multiply(s, toUInt64(9223372036854775808))) FROM (SELECT covarSampStableState(number, number) AS s FROM numbers(2));
