-- Tags: no-random-settings
-- `min` over an Array, Tuple or Map with Nullable elements must not depend on how the rows are split into blocks or aggregation states.
-- An element that is NULL compares as the largest, like in ORDER BY and inside one block, so a NULL-bearing value is never the minimum.

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
