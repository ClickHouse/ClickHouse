-- `initializeAggregation` runs the aggregate its string argument names, so an unseeded `groupArraySample` makes
-- the expression non-deterministic: a `HAVING` predicate built from it is evaluated once per group, and a call with
-- constant arguments is not folded into a single draw for the query.

SELECT 'the predicate is evaluated per group';
SELECT count() < 100 FROM (SELECT number % 100 AS g FROM numbers(10000) GROUP BY g HAVING initializeAggregation('groupArraySampleArray(1)', [g, g + 1000])[1] < 1000);

SELECT 'a constant argument is not folded into one draw';
SELECT uniqExact(x) > 1 FROM (SELECT initializeAggregation('groupArraySampleArray(1)', [1, 2, 3])[1] AS x FROM numbers(200));

SELECT 'a seeded sample stays deterministic';
SELECT uniqExact(x) FROM (SELECT initializeAggregation('groupArraySampleArray(1, 42)', [1, 2, 3])[1] AS x FROM numbers(200));

SELECT 'a deterministic aggregate still folds';
SELECT initializeAggregation('sum', 3);
SELECT uniqExact(x) FROM (SELECT initializeAggregation('max', 3) AS x FROM numbers(200));
