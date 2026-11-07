-- Negative zeros in a nested `LowCardinality` column, e.g. a typed path of `JSON`, are recognized.

SET allow_suspicious_low_cardinality_types = 1;

-- The shortened names of the two constants differ, so one constant is not substituted for the other.
SELECT 1 / if(number % 2 = 0,
    CAST(concat('{"a":-0.0,"b":"', repeat('x', 512), '"}'), 'JSON(a LowCardinality(Float64), b String)'),
    CAST(concat('{"a":0.0,"b":"', repeat('x', 512), '"}'), 'JSON(a LowCardinality(Float64), b String)')).a
FROM numbers(4)
SETTINGS compile_expressions = 0;

-- But the two zeros are the same key.
SELECT count() FROM (SELECT DISTINCT CAST(if(number % 2 = 0, '{"a":-0.0}', '{"a":0.0}'), 'JSON(a LowCardinality(Float64))') FROM numbers(4));
SELECT count() FROM (SELECT DISTINCT tuple(toLowCardinality(if(number % 2 = 0, -0., 0.))) FROM numbers(4));

-- A weighted state written before `-0.` and `0.` became equal keys can contain both of them: (0., 3), (-0., 3), (1., 4).
-- Their weights are added up when the state is read, so no observations are lost.
SELECT finalizeAggregation(s) FROM format(RowBinary, 's AggregateFunction(quantileExactWeighted(0.5), Float64, UInt64)',
    unhex('030000000000000000030000000000000000000000000000800300000000000000000000000000f03f0400000000000000'));
SELECT finalizeAggregation(s) FROM format(RowBinary, 's AggregateFunction(quantileInterpolatedWeighted(0.5), Float64, UInt64)',
    unhex('030000000000000000030000000000000000000000000000800300000000000000000000000000f03f0400000000000000'));
SELECT quantileExactWeighted(0.5)(x, w), quantileInterpolatedWeighted(0.5)(x, w) FROM values('x Float64, w UInt64', (0., 3), (-0., 3), (1., 4));
