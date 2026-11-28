-- Tags: shard
-- The results of `formatRow` and of a named `tuple` depend on aliases, which a remote server does not get (issue 130512).
-- Such queries must throw as before, and not return another value.

SET enable_positional_arguments = 1, enable_named_columns_in_function_tuple = 1, enable_parallel_replicas = 0;

SELECT 'in the positional argument';
SELECT JSONExtractUInt(toJSONString(tuple(number + 1 AS a)), 'a') AS x FROM numbers(1) ORDER BY 1;
SELECT JSONExtractUInt(toJSONString(tuple(number + 1 AS a)), 'a') AS x FROM remote('127.0.0.2', numbers(1)) ORDER BY 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT formatRowNoNewline('TSKV', max(number) + 1 AS a) AS x FROM remote('127.0.0.2', numbers(1)) ORDER BY 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT formatRowNoNewline('TSKV', (shardNum() AS h) + number AS x), x FROM remote('127.0.0.2', numbers(1)) GROUP BY 1, 2 SETTINGS serialize_query_plan = 0, optimize_group_by_function_keys = 0;

SELECT 'elsewhere in the query';
SELECT abs(number + 1 AS a) AS x, toJSONString(tuple(number + 2 AS b)) FROM remote('127.0.0.2', numbers(1)) ORDER BY 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT abs(number + 1 AS a) AS x, toJSONString(tuple(number + 2 AS b)) FROM remote('127.0.0.2', numbers(1)) ORDER BY 1 SETTINGS enable_named_columns_in_function_tuple = 0;
SELECT (SELECT abs(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(1)) ORDER BY 1), toJSONString(tuple(number + 2 AS b)) FROM remote('127.0.0.2', numbers(1)); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT abs(number * 2 AS a) AS x, number AS y, toJSONString(tuple(number AS b)) FROM remote('127.0.0.2', numbers(2)) ORDER BY x WITH FILL INTERPOLATE (y AS y + x); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SELECT 'in an ALIAS column';
DROP TABLE IF EXISTS t;
CREATE TABLE t (n UInt64, s String ALIAS formatRowNoNewline('TSKV', n + 1 AS b)) ENGINE = Memory;
INSERT INTO t VALUES (0);
SELECT abs(n + 1 AS a) AS x FROM remote('127.0.0.2', currentDatabase(), t) ORDER BY 1;
SELECT abs(n + 1 AS a) AS x, s FROM remote('127.0.0.2', currentDatabase(), t) ORDER BY 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT max(x) FROM (SELECT toString(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(1)) ORDER BY 1 UNION ALL SELECT s FROM remote('127.0.0.2', currentDatabase(), t)); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SELECT 'in an inlined view';
DROP VIEW IF EXISTS v;
CREATE VIEW v AS SELECT abs(number + 1 AS a) AS x FROM numbers(1) ORDER BY 1;
SET analyzer_inline_views = 1;
SELECT n FROM remote('127.0.0.2', currentDatabase(), t) WHERE n + 1 IN (SELECT x FROM v);
SELECT s FROM remote('127.0.0.2', currentDatabase(), t) WHERE n + 1 IN (SELECT x FROM v); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
DROP VIEW v;

SELECT 'in a row policy of an inlined view';
CREATE VIEW v AS SELECT number AS n FROM numbers(1);
CREATE ROW POLICY OR REPLACE p_05528 ON v USING n + 1 IN (SELECT abs(number + 1 AS a) AS x FROM numbers(1) ORDER BY 1) TO ALL;
SELECT n FROM remote('127.0.0.2', currentDatabase(), t) WHERE n IN (SELECT n FROM v);
SELECT s FROM remote('127.0.0.2', currentDatabase(), t) WHERE n IN (SELECT n FROM v); -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
CREATE ROW POLICY OR REPLACE p_05528 ON v USING formatRowNoNewline('TSKV', n + 1 AS b) = 'b=1' TO ALL;
SELECT abs(n + 1 AS a) AS x FROM remote('127.0.0.2', currentDatabase(), t) WHERE n IN (SELECT n FROM v) ORDER BY 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
DROP ROW POLICY p_05528 ON v;
DROP VIEW v;
DROP TABLE t;

SELECT 'grouping';
SELECT formatRowNoNewline('TSKV', number AS a) AS x, grouping(x) AS g FROM numbers(2) GROUP BY 1 WITH ROLLUP ORDER BY x, g;
SELECT formatRowNoNewline('TSKV', number AS a) AS x, grouping(formatRowNoNewline('TSKV', number AS b)) AS g FROM numbers(2) GROUP BY 1 WITH ROLLUP ORDER BY x, g; -- { serverError BAD_ARGUMENTS }
SELECT formatRowNoNewline('TSKV', number + 1 AS a), grouping(formatRowNoNewline('TSKV', number + 1 AS a)) FROM numbers(1) GROUP BY 1;
SELECT formatRowNoNewline('TSKV', number + 1 AS a), grouping(formatRowNoNewline('TSKV', number + 1 AS b)) FROM numbers(1) GROUP BY 1; -- { serverError BAD_ARGUMENTS }
SELECT toJSONString(tuple(number + 1 AS a)), grouping(toJSONString(tuple(number + 1 AS a))) FROM numbers(1) GROUP BY 1;
SELECT toJSONString(tuple(number + 1 AS a)), grouping(toJSONString(tuple(number + 1 AS b))) FROM numbers(1) GROUP BY 1; -- { serverError BAD_ARGUMENTS }
