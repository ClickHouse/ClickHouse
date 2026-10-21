-- Tags: no-parallel-replicas
-- ^ Parallel replicas run a JOIN over ARRAY JOIN on every replica, so RIGHT JOIN repeats its non-joined rows (as in #120961).

-- `PruneArrayJoinColumnsPass` must not replace the type of a reference to an ARRAY JOIN column that the analyzer
-- changed: `join_use_nulls` and `group_by_use_nulls` make it `Nullable`, `USING` gives it the common supertype.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_nested;
DROP TABLE IF EXISTS t_arr;

CREATE TABLE t_nested (`n.a` Array(Int64), `n.b` Array(Int64), `n.c` Array(Int64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested VALUES ([1, 2], [3, 4], [5, 6]);

CREATE TABLE t_arr (arr Array(Tuple(a UInt8, b String)), m Map(String, Int64), lc Nested(key LowCardinality(String), v Int64), u Nested(a UInt8, b UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_arr VALUES ([(1, 'x'), (2, 'y')], {'k1': 1, 'k2': 2}, ['Ab'], [7], [1, 2], [3, 4]);

-- `n` is pruned to `n.a`, and the ARRAY JOIN side of the RIGHT JOIN is `Nullable`.
SELECT 1 FROM t_nested ARRAY JOIN n RIGHT JOIN (SELECT 255 AS a, 2 AS b) AS r ON r.b = n.a WHERE n.a > 0 SETTINGS join_use_nulls = 1;
SELECT n.a + 1, r.b FROM t_nested ARRAY JOIN n RIGHT JOIN (SELECT 2 AS b UNION ALL SELECT 9) AS r ON r.b = n.a ORDER BY r.b SETTINGS join_use_nulls = 1;
SELECT tupleElement(n, 2) + 1, r.b FROM t_nested ARRAY JOIN n RIGHT JOIN (SELECT 2 AS b UNION ALL SELECT 9) AS r ON r.b = n.a ORDER BY r.b SETTINGS join_use_nulls = 1;
SELECT countIf(explain LIKE '%constant_value: Array_[%'), countIf(explain LIKE '%constant_value: Array_[\'a\']%')
FROM (EXPLAIN QUERY TREE SELECT n.a + 1 FROM t_nested ARRAY JOIN n RIGHT JOIN (SELECT 2 AS b) AS r ON r.b = n.a SETTINGS join_use_nulls = 1);

-- Nothing is pruned.
SELECT n.a + 1, n, r.b FROM t_nested ARRAY JOIN n RIGHT JOIN (SELECT 2 AS b UNION ALL SELECT 9) AS r ON r.b = n.a ORDER BY r.b SETTINGS join_use_nulls = 1;
SELECT kv.2 + 1, r.k FROM t_arr ARRAY JOIN m AS kv RIGHT JOIN (SELECT 1 AS k UNION ALL SELECT 9) AS r ON r.k = kv.2 ORDER BY r.k SETTINGS join_use_nulls = 1;
SELECT lower(lc.key), r.k FROM t_arr ARRAY JOIN lc RIGHT JOIN (SELECT 7 AS k UNION ALL SELECT 9) AS r ON r.k = lc.v ORDER BY r.k SETTINGS join_use_nulls = 1;
SELECT kv.2 + 1 AS v FROM t_arr ARRAY JOIN m AS kv GROUP BY kv WITH CUBE ORDER BY v SETTINGS group_by_use_nulls = 1;
SELECT tupleElement(x, 'a') + 1 FROM t_arr ARRAY JOIN arr AS x INNER JOIN (SELECT CAST((1, 'x'), 'Tuple(a UInt64, b String)') AS x) AS r USING (x);
SELECT tupleElement(u, 'a') + 1 FROM t_arr ARRAY JOIN u INNER JOIN (SELECT CAST((1, 3), 'Tuple(a UInt64, b UInt64)') AS u) AS r USING (u);

DROP TABLE t_nested;
DROP TABLE t_arr;
