-- A reference to an ARRAY JOIN column inside the default argument of tupleElement must keep the referenced subcolumn.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_nested (id UInt64, n Nested(a UInt64, b UInt64, c UInt64), m Nested(x UInt64, y UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_nested VALUES (1, [1, 2], [10, 20], [100, 200], [7, 8], [70, 80]);

SELECT n.a, n.c, tupleElement(n, 'zz', tupleElement(n, 2)) FROM t_nested ARRAY JOIN n ORDER BY 1;
SELECT tupleElement(n, 'zz', n.b) FROM t_nested ARRAY JOIN n ORDER BY 1;
SELECT tupleElement(n, 'a', n.b) FROM t_nested ARRAY JOIN n ORDER BY 1;
SELECT tupleElement(n, 'zz', n.b * 2 + 1) FROM t_nested ARRAY JOIN n ORDER BY 1;
SELECT tupleElement(n, 'zz', arrayMap(x -> x + n.b, [1])[1]) FROM t_nested ARRAY JOIN n ORDER BY 1;
SELECT tupleElement(n, 'zz', tupleElement(n, 'yy', n.c)) FROM t_nested ARRAY JOIN n ORDER BY 1;
SELECT n.c, tupleElement(n, 'zz', tupleElement(n, 3)) FROM t_nested ARRAY JOIN n ORDER BY 1;
SELECT tupleElement(n, 'zz', m.y) FROM t_nested ARRAY JOIN n, m ORDER BY 1;
SELECT n, tupleElement(n, 'zz', m.y) FROM t_nested ARRAY JOIN n, m ORDER BY 1;

DROP TABLE t_nested;
