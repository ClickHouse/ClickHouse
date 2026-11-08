-- A sorting key with a lambda must still match the same subexpression in a predicate:
-- the lambda in the key is named `lambda(tuple(x), plus(x, 1))`, like in the query.

DROP TABLE IF EXISTS t_05571;
CREATE TABLE t_05571 (arr Array(UInt64)) ENGINE = MergeTree
ORDER BY xxHash64(arrayMap(x -> x + 1, arr))
SETTINGS index_granularity = 1;
INSERT INTO t_05571 SELECT [number] FROM numbers(1000);

SELECT trim(explain) FROM (EXPLAIN indexes = 1 SELECT arr FROM t_05571 WHERE arrayMap(x -> x + 1, arr) = [2])
WHERE explain LIKE '%Granules: %/%';
SELECT arr FROM t_05571 WHERE arrayMap(x -> x + 1, arr) = [2];

DROP TABLE t_05571;
