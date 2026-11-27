-- A window function result that holds a zero-size aggregate state (an aggregate over only-NULL
-- arguments collapses to `nothing`) is copied from the previous row when the frame is the same.

SELECT 'prefix, -State';
SELECT count(), countIf(finalizeAggregation(s) = 0), uniqArrayMerge(s)
FROM (SELECT uniqArrayState([number % NULL]) OVER (PARTITION BY number % 2 ORDER BY number ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s FROM numbers(10));

SELECT 'recalculate, -State';
SELECT count(), countIf(finalizeAggregation(s) = 0)
FROM (SELECT uniqArrayState([number % NULL]) OVER (ORDER BY number % 3 RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS s FROM numbers(10));

SELECT 'any of a state';
SELECT count(), countIf(finalizeAggregation(r) = 0)
FROM (SELECT any(s) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT number AS n, uniqArrayState([number % NULL]) AS s FROM numbers(10) GROUP BY number));

SELECT 'groupArray of a state';
SELECT count(), sum(length(r))
FROM (SELECT groupArray(s) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS r FROM (SELECT number AS n, uniqArrayState([number % NULL]) AS s FROM numbers(10) GROUP BY number));

SELECT 'non-empty state';
SELECT count(), min(finalizeAggregation(s)), max(finalizeAggregation(s))
FROM (SELECT uniqState(number) OVER (ORDER BY number % 3 RANGE BETWEEN CURRENT ROW AND CURRENT ROW) AS s FROM numbers(9));
