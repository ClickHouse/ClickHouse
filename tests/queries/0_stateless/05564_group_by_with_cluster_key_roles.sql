-- A key that is both an ordinary `GROUP BY` key and a part of the `WITH CLUSTER` key would share one
-- column after aggregation and lose its exact-match role, so such queries are rejected.
-- Ordinary keys of non-comparable types (aggregate function states) are rejected, because the
-- clustering step cannot test them for equality.

SET enable_analyzer = 1;
SET allow_experimental_group_by_with_cluster = 1;

SELECT x, count() FROM VALUES('x UInt8', (1), (2)) GROUP BY x, x WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }
SELECT x, count() FROM VALUES('x UInt8', (1), (2)) GROUP BY x WITH CLUSTER 1, x; -- { serverError BAD_ARGUMENTS }
SELECT x, count() FROM VALUES('x UInt8', (1), (2)) GROUP BY x, (x, 0) WITH CLUSTER 2; -- { serverError BAD_ARGUMENTS }
SELECT s, count() FROM VALUES('s String', ('abc'), ('abd')) GROUP BY s, s WITH CLUSTER 1; -- { serverError BAD_ARGUMENTS }

SELECT finalizeAggregation(g), min(x), count()
FROM (SELECT sumState(number) AS g, number AS x FROM numbers(2) GROUP BY number)
GROUP BY g, x WITH CLUSTER 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT finalizeAggregation(g), count()
FROM (SELECT sumState(number) AS g, toString(number) AS s FROM numbers(2) GROUP BY number)
GROUP BY g, s WITH CLUSTER 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT count()
FROM (SELECT sumState(number) AS g, number AS x, number AS y FROM numbers(2) GROUP BY number)
GROUP BY g, (x, y) WITH CLUSTER 1; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Distinct expressions over the same column keep both roles.
SELECT x + 1 AS k, min(x), count() FROM VALUES('x UInt8', (1), (2)) GROUP BY x + 1, x WITH CLUSTER 1 ORDER BY k;
-- An ordinary key of a comparable type still partitions the clusters.
SELECT y, min(x), count() FROM VALUES('x UInt8, y UInt8', (1, 1), (2, 1), (3, 2), (4, 2)) GROUP BY y, x WITH CLUSTER 1 ORDER BY y;
SELECT x, min(s), count() FROM VALUES('x UInt8, s String', (1, 'abc'), (1, 'abd'), (2, 'abe')) GROUP BY x, s WITH CLUSTER 1 ORDER BY x;
