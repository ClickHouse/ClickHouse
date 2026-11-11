-- `LIMIT BY` uses the sorting if all its columns are a prefix of the sort key, where a repeated column must not count twice.

SET enable_positional_arguments = 1, max_threads = 4;

DROP TABLE IF EXISTS t;
CREATE TABLE t (a UInt8, b UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t SELECT number % 2, number % 3 FROM numbers(100000);
INSERT INTO t SELECT number % 2, number % 3 FROM numbers(100000);

SELECT count(), uniqExact(x, y) FROM (SELECT a + 1 AS x, b AS y FROM t ORDER BY 1, x LIMIT 1 BY x, y);
SELECT count(), uniqExact(x, y) FROM (SELECT a + 1 AS x, b AS y, count() FROM t GROUP BY 1, 2 ORDER BY 1, x LIMIT 1 BY x, y);

-- The sort keys are equal, but not their aliases in the lambda function.
SELECT count(), uniqExact(k, b) FROM (SELECT arrayMap(x -> (x + 1 AS z), [a]) AS k, b FROM t ORDER BY k, arrayMap(x -> x + 1, [a]) LIMIT 1 BY k, b);
SELECT count(), uniqExact(k, b) FROM (SELECT arrayMap(x -> (x + 1 AS z), [a]) AS k, b, count() FROM t GROUP BY k, b ORDER BY k, arrayMap(x -> x + 1, [a]) LIMIT 1 BY k, b);
SELECT count(), uniqExact(k, b) FROM (SELECT arrayMap(x -> (x + 1 AS z), [a]) AS k, b, count() FROM t GROUP BY 1, 2 ORDER BY 1, k LIMIT 1 BY k, b);

DROP TABLE t;
