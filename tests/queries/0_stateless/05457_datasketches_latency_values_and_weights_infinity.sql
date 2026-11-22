-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- Infinite values retained by a Quantiles sketch must still produce valid JSON.
WITH latencyValuesAndWeights(serializedQuantiles(x)) AS j
SELECT j, isValidJSON(j), JSONExtract(j, 'values', 'Array(Float64)')
FROM (SELECT arrayJoin([1., inf, -inf, 2.5]) AS x);
