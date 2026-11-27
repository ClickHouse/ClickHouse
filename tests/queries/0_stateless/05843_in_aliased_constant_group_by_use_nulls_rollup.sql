-- An aliased tuple constant that is both a GROUP BY key and the set of `IN`, with `group_by_use_nulls`.
SELECT (3, 6) AS c, number IN c AS f, count() FROM numbers(10) GROUP BY c, f WITH ROLLUP ORDER BY ALL SETTINGS group_by_use_nulls = 1;
