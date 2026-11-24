-- `y.a` after `ARRAY JOIN arr AS y` over an `Array(Tuple)` column must not read the unused elements of `arr`.

SET enable_parallel_replicas = 0;
SET serialize_query_plan = 0;
SET optimize_functions_to_subcolumns = 1;

DROP TABLE IF EXISTS t_array_tuple;
CREATE TABLE t_array_tuple (id UInt64, arr Array(Tuple(a UInt64, b String, c String, d DateTime64(9, 'UTC')))) ENGINE = ReplacingMergeTree ORDER BY id;
INSERT INTO t_array_tuple VALUES (1, [(1, 'b1', 'c1', '2026-08-20 00:00:00'), (2, 'b2', 'c2', '2026-08-21 00:00:00')]), (2, []), (3, [(3, 'b3', 'c3', '2026-08-22 00:00:00')]);

SELECT '-- two elements';
SELECT y.c, sum(y.a) FROM t_array_tuple AS x ARRAY JOIN x.arr AS y WHERE y.d > toDateTime('2026-08-20 12:00:00', 'UTC') GROUP BY y.c ORDER BY 1;
SELECT extract(explain, 'arr(?:\\.\\w+)?') FROM (EXPLAIN header = 1 SELECT y.c, sum(y.a) FROM t_array_tuple AS x ARRAY JOIN x.arr AS y WHERE y.d > toDateTime('2026-08-20 12:00:00', 'UTC') GROUP BY y.c) WHERE match(explain, '^\\W*(Header: )?arr\\b') ORDER BY 1;

SELECT '-- tupleElement by name and by index';
SELECT tupleElement(y, 'b'), tupleElement(y, 3) FROM t_array_tuple ARRAY JOIN arr AS y ORDER BY 1;
SELECT extract(explain, 'arr(?:\\.\\w+)?') FROM (EXPLAIN header = 1 SELECT tupleElement(y, 'b'), tupleElement(y, 3) FROM t_array_tuple ARRAY JOIN arr AS y) WHERE match(explain, '^\\W*(Header: )?arr\\b') ORDER BY 1;

SELECT '-- LEFT ARRAY JOIN';
SELECT id, y.a FROM t_array_tuple LEFT ARRAY JOIN arr AS y ORDER BY id, y.a;
SELECT extract(explain, 'arr(?:\\.\\w+)?') FROM (EXPLAIN header = 1 SELECT id, y.a FROM t_array_tuple LEFT ARRAY JOIN arr AS y) WHERE match(explain, '^\\W*(Header: )?arr\\b') ORDER BY 1;

SELECT '-- no alias';
SELECT arr.b FROM t_array_tuple ARRAY JOIN arr ORDER BY 1;
SELECT extract(explain, 'arr(?:\\.\\w+)?') FROM (EXPLAIN header = 1 SELECT arr.b FROM t_array_tuple ARRAY JOIN arr) WHERE match(explain, '^\\W*(Header: )?arr\\b') ORDER BY 1;

SELECT '-- only the row count';
SELECT count() FROM t_array_tuple ARRAY JOIN arr AS y;
SELECT extract(explain, 'arr(?:\\.\\w+)?') FROM (EXPLAIN header = 1 SELECT count() FROM t_array_tuple ARRAY JOIN arr AS y) WHERE match(explain, '^\\W*(Header: )?arr\\b') ORDER BY 1;

SELECT '-- the query uses the whole tuple';
SELECT y.a, y FROM t_array_tuple ARRAY JOIN arr AS y ORDER BY 1;
SELECT extract(explain, 'arr(?:\\.\\w+)?') FROM (EXPLAIN header = 1 SELECT y.a, y FROM t_array_tuple ARRAY JOIN arr AS y) WHERE match(explain, '^\\W*(Header: )?arr\\b') ORDER BY 1;

SELECT '-- the query also uses the array';
SELECT y.a, length(arr) FROM t_array_tuple ARRAY JOIN arr AS y ORDER BY 1;

SELECT '-- FINAL';
SELECT y.a, y.b FROM t_array_tuple FINAL ARRAY JOIN arr AS y ORDER BY 1;

SELECT '-- join_use_nulls';
SELECT y.a + 1, r.k FROM t_array_tuple ARRAY JOIN arr AS y RIGHT JOIN (SELECT 2 AS k UNION ALL SELECT 9) AS r ON r.k = y.a ORDER BY r.k SETTINGS join_use_nulls = 1;

SELECT '-- the same results without the rewrite';
SELECT y.c, sum(y.a), groupArray(tupleElement(y, 3)) FROM t_array_tuple ARRAY JOIN arr AS y GROUP BY y.c ORDER BY 1 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT y.c, sum(y.a), groupArray(tupleElement(y, 3)) FROM t_array_tuple ARRAY JOIN arr AS y GROUP BY y.c ORDER BY 1;

SELECT '-- tupleElement over the array without ARRAY JOIN';
SELECT tupleElement(arr, 'a') FROM t_array_tuple ORDER BY id;
SELECT extract(explain, 'arr(?:\\.\\w+)?') FROM (EXPLAIN header = 1 SELECT tupleElement(arr, 'a') FROM t_array_tuple) WHERE match(explain, '^\\W*(Header: )?arr\\b') ORDER BY 1;

DROP TABLE t_array_tuple;
