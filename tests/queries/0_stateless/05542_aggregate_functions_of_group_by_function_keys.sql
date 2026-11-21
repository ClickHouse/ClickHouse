-- Tags: shard
-- Aggregate functions of `GROUP BY` keys, and functions of keys, that cannot be replaced with the key, or calculated from it.

SET optimize_aggregators_of_group_by_keys = 1, optimize_group_by_function_keys = 1;

SELECT 'not deterministic';
SELECT [number, number + 1] AS a, min(arrayJoin(a)) AS m FROM numbers(2) GROUP BY a ORDER BY ALL;
SELECT [number, number + 1] AS a, min(arrayJoin(a)) AS m FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY a ORDER BY ALL;
SELECT number % 3 AS a FROM numbers(6) GROUP BY a HAVING max(arrayJoin([a, 7])) = 7 ORDER BY ALL;
SELECT number % 3 AS a, min(rand64(a)) < 922337203685477580 AS m FROM numbers(3000) GROUP BY a ORDER BY ALL;

SELECT 'WITH FILL';
SELECT negate(number % 3) AS x1, max(x1) AS x2 FROM numbers(9) GROUP BY x1 ORDER BY x2 WITH FILL FROM -2 TO 3;
-- It is still replaced in HAVING, and the filter uses the primary key.
DROP TABLE IF EXISTS t;
CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t VALUES (0), (2), (4);
SELECT k, count() FROM t GROUP BY k HAVING max(k) > 0 ORDER BY k WITH FILL SETTINGS force_primary_key = 1, enable_parallel_replicas = 0;
SELECT k, max(k) AS m FROM t GROUP BY k HAVING m > 0 ORDER BY k WITH FILL SETTINGS force_primary_key = 1, enable_parallel_replicas = 0;
SELECT k + 1 AS x, max(k) + 1 AS m FROM t GROUP BY k HAVING m > 1 ORDER BY x WITH FILL SETTINGS force_primary_key = 1, enable_parallel_replicas = 0;
SELECT countIf(r % 2 = 1) FROM (SELECT number AS k, rand(max(k)) AS r FROM numbers(100) GROUP BY k HAVING r % 2 = 0 ORDER BY k WITH FILL);
SELECT countSubstrings(explain, 'max(') FROM (EXPLAIN SYNTAX oneline = 1, run_query_tree_passes = 1 WITH x -> (x + 1 AS y) * 2 AS f SELECT k, f(max(k)) AS m FROM t GROUP BY k HAVING m > 0 ORDER BY k WITH FILL);
DROP TABLE t;

SELECT 'types with a custom name';
SELECT (number % 2)::Bool AS k, groupBitOr(k) AS o, toTypeName(o) FROM numbers(4) GROUP BY k ORDER BY k;
SELECT number % 2 = 1 AS k, max(k) AS m, toTypeName(m) FROM numbers(4) GROUP BY k ORDER BY k;

SELECT 'arrayJoin';
SELECT [number, number + 5] AS k, arrayJoin(k) AS x, arrayJoin(max(k)) AS y FROM numbers(1) GROUP BY k ORDER BY ALL;
SELECT [number, number + 5] AS k, arrayJoin(k) AS x FROM numbers(1) GROUP BY k HAVING arrayJoin([max(k)[1], 5]) = 0 ORDER BY ALL;

SELECT 'grouping';
SELECT number + 1 AS a, toString(a) AS b, grouping(b) AS g FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY a, b ORDER BY a;
SELECT number + 1 AS a, toString(a) AS b, count() FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY a, b HAVING grouping(b) = 0 ORDER BY a;
SELECT number AS a, ((number, 2), 2) AS b, grouping(a) AS g FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY a, b ORDER BY a;

SELECT 'positional arguments with aliases';
SET enable_positional_arguments = 1, optimize_limit_by_function_keys = 1;
SELECT (SELECT groupArray(x + m) FROM (SELECT number + 1 AS x, max(arrayJoin([x])) AS m FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY 1 ORDER BY 1, 2 LIMIT 2));
SELECT (SELECT (count(), sum(y - x)) FROM (SELECT number AS y, negate(abs(y) AS a) AS x FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY x, y WITH ROLLUP LIMIT 1 BY y, 2));
SELECT number * 2 AS x, abs(max(number + 1 AS a)) FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY number ORDER BY 1 WITH FILL, 2 INTERPOLATE (a AS a + 1);
SELECT number, max(formatRowNoNewline('TSKV', number + 1 AS z)) FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY number ORDER BY number WITH FILL, 2 SETTINGS serialize_query_plan = 0;
SELECT 'self join';
SELECT b.number AS y, negate(a.number) AS x, negate(b.number) AS z FROM numbers(2) AS a CROSS JOIN numbers(2) AS b GROUP BY y, z, x WITH ROLLUP ORDER BY y, z, x LIMIT 1 BY 1, 3;
SELECT 'time zones';
SELECT number AS y, toHour(toDateTime(0, 'UTC') + y * 3600) AS h, toHour(toDateTime(0, 'Asia/Tokyo') + y * 3600) AS k FROM numbers(2) GROUP BY k, y WITH ROLLUP ORDER BY y, k LIMIT 1 BY 1, 3;
