-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- A `NaN` percentile must be rejected before it reaches the sketch library.

SELECT percentileFromQuantiles(serializedQuantiles(number), nan) FROM numbers(2); -- { serverError BAD_ARGUMENTS }
SELECT percentileFromQuantiles(s, materialize(nan)) FROM (SELECT serializedQuantiles(number) AS s FROM numbers(2)); -- { serverError BAD_ARGUMENTS }
SELECT percentileFromTDigest(serializedTDigest(number), nan) FROM numbers(2); -- { serverError BAD_ARGUMENTS }
SELECT percentileFromTDigest(s, materialize(nan)) FROM (SELECT serializedTDigest(number) AS s FROM numbers(2)); -- { serverError BAD_ARGUMENTS }

SELECT percentileFromQuantiles(s, materialize(0.5)) FROM (SELECT serializedQuantiles(number) AS s FROM numbers(3));
SELECT percentileFromTDigest(s, materialize(0.5)) FROM (SELECT serializedTDigest(number) AS s FROM numbers(3));
