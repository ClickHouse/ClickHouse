-- Tags: no-random-settings
-- `min`, `max`, `argMin` and `argMax` over an Array, Tuple or Map must not depend on how the rows are split into blocks or aggregation states.
-- Inside one block an element that is NULL or NaN never makes the minimum or the maximum, and the same holds when blocks or states are combined.

SELECT 'one block';
SELECT min(a), max(a) FROM (SELECT [if(number % 2 = 0, NULL, 0)]::Array(Nullable(UInt8)) AS a FROM numbers(4)) SETTINGS max_threads = 1;

SELECT 'NULL-bearing rows in their own block';
SELECT min(a), max(a) FROM (SELECT [if(number < 3, NULL, 0)]::Array(Nullable(UInt8)) AS a FROM numbers(6)) SETTINGS max_threads = 1, max_block_size = 3;
SELECT min(a), max(a) FROM (SELECT [if(number < 3, 0, NULL)]::Array(Nullable(UInt8)) AS a FROM numbers(6)) SETTINGS max_threads = 1, max_block_size = 3;

SELECT 'merge of states';
SELECT minMerge(s), maxMerge(t)
FROM (SELECT minState(a) AS s, maxState(a) AS t FROM (SELECT [if(number % 2 = 0, NULL, 0)]::Array(Nullable(UInt8)) AS a FROM numbers(4)) GROUP BY a[1] IS NULL);

SELECT 'Tuple';
SELECT min(a), max(a) FROM (SELECT (0, if(number < 3, NULL, 0))::Tuple(UInt8, Nullable(UInt8)) AS a FROM numbers(6)) SETTINGS max_threads = 1, max_block_size = 3;
SELECT minMerge(s), maxMerge(t)
FROM (SELECT minState(a) AS s, maxState(a) AS t FROM (SELECT (0, if(number % 2 = 0, NULL, 0))::Tuple(UInt8, Nullable(UInt8)) AS a FROM numbers(4)) GROUP BY a.2 IS NULL);

SELECT 'Map';
SELECT min(a), max(a) FROM (SELECT map(0, if(number < 3, NULL, 0))::Map(UInt8, Nullable(UInt8)) AS a FROM numbers(6)) SETTINGS max_threads = 1, max_block_size = 3;
SELECT minMerge(s), maxMerge(t)
FROM (SELECT minState(a) AS s, maxState(a) AS t FROM (SELECT map(0, if(number % 2 = 0, NULL, 0))::Map(UInt8, Nullable(UInt8)) AS a FROM numbers(4)) GROUP BY a[0] IS NULL);

SELECT 'MergeTree parts';
DROP TABLE IF EXISTS t_min_nullable_array;
CREATE TABLE t_min_nullable_array (a Array(Nullable(UInt8))) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t_min_nullable_array;
INSERT INTO t_min_nullable_array SELECT [NULL] FROM numbers(10);
INSERT INTO t_min_nullable_array SELECT [0] FROM numbers(10);
SELECT min(a), max(a) FROM t_min_nullable_array SETTINGS max_threads = 1;
SELECT min(a), max(a) FROM t_min_nullable_array SETTINGS max_threads = 4;
DROP TABLE t_min_nullable_array;

SELECT 'NaN';
SELECT max(a), min(a) FROM (SELECT [if(number < 3, nan, 2.)] AS a FROM numbers(6)) SETTINGS max_threads = 1, max_block_size = 3;
SELECT max(a), min(a) FROM (SELECT [if(number < 3, 2., nan)] AS a FROM numbers(6)) SETTINGS max_threads = 1, max_block_size = 3;
SELECT max(a), min(a) FROM (SELECT (if(number < 3, nan, 2.), 1.)::Tuple(Float64, Float64) AS a FROM numbers(6)) SETTINGS max_threads = 1, max_block_size = 3;
SELECT max(a), min(a) FROM (SELECT [if(number < 3, nan, 2.), 0.] AS a FROM numbers(6)) SETTINGS max_threads = 1, max_block_size = 3;
SELECT maxMerge(s), minMerge(t)
FROM (SELECT maxState(a) AS s, minState(a) AS t FROM (SELECT [if(number % 2 = 0, nan, 2.)] AS a FROM numbers(4)) GROUP BY isNaN(a[1]));

SELECT 'argMax and argMin';
SELECT argMax(number, a), argMin(number, a) FROM (SELECT [if(number < 3, nan, 2. + number)] AS a, number FROM numbers(6)) SETTINGS max_threads = 1, max_block_size = 3;
SELECT argMax(number, a), argMin(number, a) FROM (SELECT [if(number < 3, nan, 2. + number)] AS a, number FROM numbers(6)) SETTINGS max_threads = 1, max_block_size = 65536;
