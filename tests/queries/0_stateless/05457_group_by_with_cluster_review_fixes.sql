SET allow_experimental_group_by_with_cluster = 1;
SET enable_analyzer = 1;

-- The gap between the keys (2.2e-16) exceeds the distance, even though `1.0 + 1.5e-16` rounds to the second key.
SELECT x, count() FROM VALUES('x Float64', (1.0), (1.0000000000000002)) GROUP BY x WITH CLUSTER 1.5e-16 ORDER BY x;

-- `HAVING` is applied after clustering regardless of `analyzer_compatibility_allow_non_aggregate_in_having`.
SELECT x, count() FROM VALUES('x UInt8', (1), (2)) GROUP BY x WITH CLUSTER 1 HAVING x > 1 SETTINGS analyzer_compatibility_allow_non_aggregate_in_having = 0;
SELECT x, count() FROM VALUES('x UInt8', (1), (2)) GROUP BY x WITH CLUSTER 1 HAVING x > 1 SETTINGS analyzer_compatibility_allow_non_aggregate_in_having = 1;
SELECT x, count() FROM VALUES('x UInt8', (1), (2), (5)) GROUP BY x WITH CLUSTER 1 HAVING x < 3 SETTINGS analyzer_compatibility_allow_non_aggregate_in_having = 0;
SELECT x, count() FROM VALUES('x UInt8', (1), (2), (5)) GROUP BY x WITH CLUSTER 1 HAVING x < 3 SETTINGS analyzer_compatibility_allow_non_aggregate_in_having = 1;

-- A long chain of single-key buckets is merged in linear time.
SELECT number, count() FROM numbers(1000000) GROUP BY number WITH CLUSTER 1;

-- The bounded edit distance check.
SELECT s, count() FROM VALUES('s String', ('kitten'), ('sitting')) GROUP BY s WITH CLUSTER 2 ORDER BY s;
SELECT s, count() FROM VALUES('s String', ('kitten'), ('sitting')) GROUP BY s WITH CLUSTER 3 ORDER BY s;
SELECT s, count() FROM VALUES('s String', (''), ('ab'), ('xyz')) GROUP BY s WITH CLUSTER 2 ORDER BY s;
SELECT length(s), count() FROM (SELECT arrayStringConcat(arrayMap(i -> toString(i % 10), range(n))) || suffix AS s FROM VALUES('n UInt32, suffix String', (1000, ''), (1000, 'abc'), (997, 'abcdef')))
GROUP BY s WITH CLUSTER 3 ORDER BY length(s);
