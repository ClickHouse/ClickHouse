-- A window aggregate result that holds aggregate function states inside an empty `Map` or `Array`
-- is copied from the previous row when the frame is the same.

SELECT 'prefix, Map';
SELECT g, mapKeys(m), arrayMap(x -> finalizeAggregation(x), mapValues(m)), count()
FROM (SELECT number % 2 AS g, countStateMap(g) OVER (ORDER BY g GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS m FROM numbers(10))
GROUP BY ALL ORDER BY g;

SELECT 'recalculate, Map';
SELECT g, mapKeys(m), arrayMap(x -> finalizeAggregation(x), mapValues(m)), count()
FROM (SELECT number % 2 AS g, countStateMap(g) OVER (ORDER BY g GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS m FROM numbers(10))
GROUP BY ALL ORDER BY g;

SELECT 'prefix, empty Map argument';
SELECT count(), countIf(empty(m))
FROM (SELECT sumStateMap(CAST(map(), 'Map(UInt8, UInt64)')) OVER () AS m FROM numbers(10));

SELECT 'prefix, Array';
SELECT g, arrayMap(x -> finalizeAggregation(x), a), count()
FROM (SELECT number % 2 AS g, sumStateForEach(if(g = 0, [], [number])) OVER (ORDER BY g GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS a FROM numbers(10))
GROUP BY ALL ORDER BY g;

SELECT 'groupArray of a state';
SELECT g, length(r), count()
FROM (SELECT g, groupArray(s) OVER (ORDER BY g GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS r
      FROM (SELECT number % 2 AS g, uniqState(number) AS s FROM numbers(10) GROUP BY number))
GROUP BY ALL ORDER BY g;
