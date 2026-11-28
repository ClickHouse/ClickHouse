-- Tags: shard
-- The query sent to a remote server must have the aliases of a positional argument that the remote server resolves again.

SET enable_positional_arguments = 1;

SELECT 'INTERPOLATE';
SELECT abs(number * 2 AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1 WITH FILL INTERPOLATE (a AS a + 1);
SELECT abs(number * 2 AS a) AS x, number AS y FROM remote('127.0.0.2', numbers(2)) ORDER BY 1 WITH FILL INTERPOLATE (y AS y + x + 5);
SELECT abs(number * 2 AS a) AS x, number AS y FROM remote('127.0.0.2', numbers(2)) ORDER BY x WITH FILL INTERPOLATE (y AS y + x + 5);

SELECT 'JOIN USING';
SET analyzer_compatibility_join_using_top_level_identifier = 1;
-- The remote server finds the key `number` among the nested aliases, as the local one does.
SELECT abs(l.number + 10 AS number) FROM remote('127.0.0.{1,2}', numbers(2)) AS l JOIN (SELECT 11 AS number) AS r USING (number) ORDER BY 1;
SELECT abs(l.number + 10 AS number) AS x FROM remote('127.0.0.{1,2}', numbers(2)) AS l JOIN (SELECT 11 AS number) AS r USING (number) ORDER BY 1;
-- A repeated alias is not a key.
SELECT abs(l.number + 10 AS number) AS x, abs(l.number + 20 AS number) AS y FROM remote('127.0.0.{1,2}', numbers(2)) AS l JOIN (SELECT 1 AS number) AS r USING (number) ORDER BY 1;
SELECT number + 1 AS x FROM remote('127.0.0.2', numbers(1)) AS l FULL JOIN (SELECT toUInt64(1) AS number) AS r USING (number) ORDER BY 1;
