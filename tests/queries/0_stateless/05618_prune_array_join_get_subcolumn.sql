-- `n.x` after `ARRAY JOIN nst AS n` is `getSubcolumn(n, 'x')`: the query must not read the unused elements of `nst`.

SET enable_parallel_replicas = 0;
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_nested (nst Nested(x UInt64, y String, z Array(UInt64))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested VALUES ([1, 2], ['a', 'b'], [[1], [2, 3]]), ([], [], []), ([3], ['c'], [[]]);

SELECT '-- one subcolumn';
SELECT sum(n.x) FROM t_nested ARRAY JOIN nst AS n;
SELECT extract(explain, 'nst\\.\\w+') FROM (EXPLAIN header = 1 SELECT sum(n.x) FROM t_nested ARRAY JOIN nst AS n) WHERE match(explain, '^\\W*(Header: )?nst\\.');

SELECT '-- LEFT ARRAY JOIN';
SELECT n.x FROM t_nested LEFT ARRAY JOIN nst AS n ORDER BY 1;
SELECT extract(explain, 'nst\\.\\w+') FROM (EXPLAIN header = 1 SELECT n.x FROM t_nested LEFT ARRAY JOIN nst AS n) WHERE match(explain, '^\\W*(Header: )?nst\\.');

SELECT '-- subcolumn of an element';
SELECT n.z.size0 FROM t_nested ARRAY JOIN nst AS n ORDER BY 1;
SELECT extract(explain, 'nst\\.\\w+') FROM (EXPLAIN header = 1 SELECT n.z.size0 FROM t_nested ARRAY JOIN nst AS n) WHERE match(explain, '^\\W*(Header: )?nst\\.');

SELECT '-- subcolumn of an element, no alias';
SELECT nst.z.size0 FROM t_nested ARRAY JOIN nst ORDER BY 1;
SELECT extract(explain, 'nst\\.\\w+') FROM (EXPLAIN header = 1 SELECT nst.z.size0 FROM t_nested ARRAY JOIN nst) WHERE match(explain, '^\\W*(Header: )?nst\\.');

SELECT '-- getSubcolumn and tupleElement';
SELECT n.x, tupleElement(n, 'y') FROM t_nested ARRAY JOIN nst AS n ORDER BY 1;
SELECT extract(explain, 'nst\\.\\w+') FROM (EXPLAIN header = 1 SELECT n.x, tupleElement(n, 'y') FROM t_nested ARRAY JOIN nst AS n) WHERE match(explain, '^\\W*(Header: )?nst\\.');

SELECT '-- the query uses the whole tuple';
SELECT n.x, n FROM t_nested ARRAY JOIN nst AS n ORDER BY 1;
SELECT extract(explain, 'nst\\.\\w+') FROM (EXPLAIN header = 1 SELECT n.x, n FROM t_nested ARRAY JOIN nst AS n) WHERE match(explain, '^\\W*(Header: )?nst\\.');

SELECT '-- subquery';
SELECT * FROM (SELECT n.y FROM t_nested ARRAY JOIN nst AS n) ORDER BY 1;
SELECT extract(explain, 'nst\\.\\w+') FROM (EXPLAIN header = 1 SELECT * FROM (SELECT n.y FROM t_nested ARRAY JOIN nst AS n)) WHERE match(explain, '^\\W*(Header: )?nst\\.');

SELECT '-- join_use_nulls';
SELECT n.x + 1, r.k FROM t_nested ARRAY JOIN nst AS n RIGHT JOIN (SELECT 2 AS k UNION ALL SELECT 9) AS r ON r.k = n.x ORDER BY r.k SETTINGS join_use_nulls = 1;
SELECT extract(explain, 'nst\\.\\w+') FROM (EXPLAIN header = 1 SELECT n.x + 1, r.k FROM t_nested ARRAY JOIN nst AS n RIGHT JOIN (SELECT 2 AS k) AS r ON r.k = n.x SETTINGS join_use_nulls = 1) WHERE match(explain, '^\\W*(Header: )?nst\\.');

DROP TABLE t_nested;

-- the type resolves `k.a.b` to the element `a.b`, not to a subcolumn of the element `a`.
DROP TABLE IF EXISTS t_dots;
CREATE TABLE t_dots (`d.a` Array(String), `d.a.b` Array(UInt64), `d.c` Array(UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dots VALUES (['p', 'q'], [1, 2], [7, 8]);

SELECT '-- element name with a dot';
SELECT k.a.b FROM t_dots ARRAY JOIN d AS k ORDER BY 1;
SELECT extract(explain, 'd\\.[\\w.]+') FROM (EXPLAIN header = 1 SELECT k.a.b FROM t_dots ARRAY JOIN d AS k) WHERE match(explain, '^\\W*(Header: )?d\\.');

DROP TABLE t_dots;
