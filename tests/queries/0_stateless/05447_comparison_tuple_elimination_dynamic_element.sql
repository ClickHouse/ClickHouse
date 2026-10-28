-- Comparing a tuple of columns with a constant tuple keeps the member type of a `Dynamic` element.
DROP TABLE IF EXISTS t_tuple_dynamic;
CREATE TABLE t_tuple_dynamic (d DateTime('UTC'), x UInt64, s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_tuple_dynamic VALUES ('2020-01-01 00:00:00', 1, 'a');

SELECT count() FROM t_tuple_dynamic WHERE (d, x) = (CAST(toDate('2020-01-01') AS Dynamic), 1);
SELECT (d, x) = (CAST(toDate('2020-01-01') AS Dynamic), 1) FROM t_tuple_dynamic;
SELECT count() FROM t_tuple_dynamic WHERE (d, x) != (CAST(toDate('2020-01-01') AS Dynamic), 1);
SELECT count() FROM t_tuple_dynamic WHERE tuple(d) = tuple(CAST(toDate('2020-01-01') AS Dynamic));
SELECT count() FROM t_tuple_dynamic WHERE (x, (d, s)) = (1, (CAST(toDate('2020-01-01') AS Dynamic), 'a'));
SELECT count() FROM t_tuple_dynamic WHERE (d, x) = (CAST(toDateTime64('2020-01-01 00:00:00', 3, 'UTC') AS Dynamic), 1);
SELECT count() FROM t_tuple_dynamic WHERE (x, s) = (1, CAST(CAST('a' AS Enum8('a' = 1, 'b' = 2)) AS Dynamic));
SELECT count() FROM (SELECT [toDate('2020-01-01')] AS a, 1 AS x) WHERE (a, x) = (CAST([toDate('2020-01-01')] AS Array(Dynamic)), 1);
SELECT count() FROM (SELECT map('k', toDate('2020-01-01')) AS m, 1 AS x) WHERE (m, x) = (CAST(map('k', toDate('2020-01-01')) AS Map(String, Dynamic)), 1);
SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_tuple_dynamic) WHERE (d, x) = (CAST(toDate('2020-01-01') AS Dynamic), 1);

-- The comparison is still rewritten into a conjunction.
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM t_tuple_dynamic WHERE (d, x) = (CAST(toDate('2020-01-01') AS Dynamic), 1)) WHERE explain ILIKE '%function_name: and%';

DROP TABLE t_tuple_dynamic;
