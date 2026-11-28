-- minDistinct and maxDistinct are resolved as min and max: duplicates do not change the result.
SET enable_analyzer = 1;

SELECT minDistinct(number % 5), maxDistinct(number % 5), MIN(DISTINCT number % 5), max(DISTINCT number % 5) FROM numbers(100);
SELECT minDistinctIf(number % 5, number > 50), maxIfDistinct(number % 5, number < 3) FROM numbers(100);
SELECT minDistinct(x), maxDistinct(x), toTypeName(minDistinct(x)) FROM (SELECT if(number % 3 = 0, NULL, number) AS x FROM numbers(10));
SELECT minDistinct(toLowCardinality(toString(number))), toTypeName(maxDistinct(toLowCardinality(toString(number)))) FROM numbers(10);
SELECT minDistinct(number) FROM numbers(0) SETTINGS aggregate_functions_null_for_empty = 1;
SELECT minDistinct(number), maxDistinct(number) FROM remote('127.0.0.{1,2}', numbers(10));
SELECT number, minDistinct(number) OVER (ORDER BY number DESC) FROM numbers(4) ORDER BY number;

SELECT toTypeName(minDistinctState(number)), finalizeAggregation(maxDistinctState(number)) FROM numbers(10);
SELECT minDistinctMerge(s) FROM (SELECT minDistinctState(number) AS s FROM numbers(10));

SELECT trim(explain) FROM (EXPLAIN QUERY TREE SELECT minDistinct(number), MAX(DISTINCT number), maxIfDistinct(number, number > 1), minDistinctState(number) FROM numbers(3)) WHERE explain LIKE '%function_type: aggregate%';
