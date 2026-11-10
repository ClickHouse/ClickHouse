-- `n.x` after `ARRAY JOIN nst AS n` is `getSubcolumn(n, 'x')`: the query must not read the unused elements of `nst`.

SET enable_parallel_replicas = 0;
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_nested (nst Nested(x UInt64, y String, z Array(UInt64))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested VALUES ([1, 2], ['a', 'b'], [[1], [2, 3]]), ([], [], []), ([3], ['c'], [[]]);

SELECT '-- one subcolumn';
SELECT sum(n.x) FROM t_nested ARRAY JOIN nst AS n;
SELECT replaceRegexpOne(explain, '^[\\s│└─]*(Header: )?', '') FROM (EXPLAIN header = 1 SELECT sum(n.x) FROM t_nested ARRAY JOIN nst AS n) WHERE match(explain, '^[\\s│└─]*(Header: )?nst\\.');

SELECT '-- LEFT ARRAY JOIN';
SELECT n.x FROM t_nested LEFT ARRAY JOIN nst AS n ORDER BY 1;
SELECT replaceRegexpOne(explain, '^[\\s│└─]*(Header: )?', '') FROM (EXPLAIN header = 1 SELECT n.x FROM t_nested LEFT ARRAY JOIN nst AS n) WHERE match(explain, '^[\\s│└─]*(Header: )?nst\\.');

SELECT '-- subcolumn of an element';
SELECT n.z.size0 FROM t_nested ARRAY JOIN nst AS n ORDER BY 1;
SELECT replaceRegexpOne(explain, '^[\\s│└─]*(Header: )?', '') FROM (EXPLAIN header = 1 SELECT n.z.size0 FROM t_nested ARRAY JOIN nst AS n) WHERE match(explain, '^[\\s│└─]*(Header: )?nst\\.');

SELECT '-- getSubcolumn and tupleElement';
SELECT n.x, tupleElement(n, 'y') FROM t_nested ARRAY JOIN nst AS n ORDER BY 1;
SELECT replaceRegexpOne(explain, '^[\\s│└─]*(Header: )?', '') FROM (EXPLAIN header = 1 SELECT n.x, tupleElement(n, 'y') FROM t_nested ARRAY JOIN nst AS n) WHERE match(explain, '^[\\s│└─]*(Header: )?nst\\.');

SELECT '-- the query uses the whole tuple';
SELECT n.x, n FROM t_nested ARRAY JOIN nst AS n ORDER BY 1;
SELECT replaceRegexpOne(explain, '^[\\s│└─]*(Header: )?', '') FROM (EXPLAIN header = 1 SELECT n.x, n FROM t_nested ARRAY JOIN nst AS n) WHERE match(explain, '^[\\s│└─]*(Header: )?nst\\.');

SELECT '-- subquery';
SELECT * FROM (SELECT n.y FROM t_nested ARRAY JOIN nst AS n) ORDER BY 1;
SELECT replaceRegexpOne(explain, '^[\\s│└─]*(Header: )?', '') FROM (EXPLAIN header = 1 SELECT * FROM (SELECT n.y FROM t_nested ARRAY JOIN nst AS n)) WHERE match(explain, '^[\\s│└─]*(Header: )?nst\\.');

SELECT '-- join_use_nulls';
SELECT n.x + 1, r.k FROM t_nested ARRAY JOIN nst AS n RIGHT JOIN (SELECT 2 AS k UNION ALL SELECT 9) AS r ON r.k = n.x ORDER BY r.k SETTINGS join_use_nulls = 1;
SELECT replaceRegexpOne(explain, '^[\\s│└─]*(Header: )?', '') FROM (EXPLAIN header = 1 SELECT n.x + 1, r.k FROM t_nested ARRAY JOIN nst AS n RIGHT JOIN (SELECT 2 AS k) AS r ON r.k = n.x SETTINGS join_use_nulls = 1) WHERE match(explain, '^[\\s│└─]*(Header: )?nst\\.');

DROP TABLE t_nested;
