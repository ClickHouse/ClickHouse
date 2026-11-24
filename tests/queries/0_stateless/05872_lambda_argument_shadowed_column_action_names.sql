-- The names of the actions that use a column shadowed by a lambda argument are the same for the actions
-- and for their consumers: the descriptions of aggregate functions, window functions, and sorting.

DROP TABLE IF EXISTS t_shadowed_names;
CREATE TABLE t_shadowed_names (value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadowed_names VALUES (10), (20);

-- A quoted lambda argument may coincide with the column identifier of a table column, here `__table1.value`.
SELECT sum(arrayMap(`__table1.value` -> t.value + `__table1.value`, [toUInt8(1)])[1]) FROM t_shadowed_names AS t;
SELECT t.value FROM t_shadowed_names AS t ORDER BY arrayMap(`__table1.value` -> t.value + `__table1.value`, [toUInt8(1)])[1] DESC;
SELECT arrayMap(`__table1.value` -> t.value + `__table1.value`, [toUInt8(1)])[1] AS k, count() FROM t_shadowed_names AS t GROUP BY k ORDER BY k;
SELECT max(arrayMap(`__table1.value` -> t.value + `__table1.value`, [toUInt8(1)])[1])
    OVER (ORDER BY arrayMap(`__table1.value` -> t.value * `__table1.value`, [toUInt8(2)])[1]) AS m
FROM t_shadowed_names AS t ORDER BY m;

-- A `GROUP BY` key and an aggregate function in the body of the lambda are inputs of the actions.
SELECT arrayMap(`__table1.value` -> (t.value + 1) + `__table1.value`, [toUInt8(1)])[1] AS k, count() FROM t_shadowed_names AS t GROUP BY t.value + 1 ORDER BY k;
SELECT arrayMap(`__table1.value` -> sum(t.value) + `__table1.value`, [toUInt8(1)])[1] FROM t_shadowed_names AS t;

-- The lambda with the shadowed column and the lambda without it are different actions. 11 + 2 = 13.
SELECT value FROM t_shadowed_names AS t PREWHERE arrayMap(value -> t.value + value, [1])[1] + arrayMap(value -> value + value, [1])[1] = 13;

-- The argument of the nested lambda shadows the column as well. (10 + 1) + 10 = 21.
SELECT value FROM t_shadowed_names AS t PREWHERE arrayMap(value -> arrayMap(value -> toUInt8(t.value) + value, [value])[1] + t.value, [1])[1] = 21;

DROP TABLE t_shadowed_names;

-- A column may be named like a shadowed column is named inside a lambda. 100 + 10 + 1 = 111, 100 + 10 + 1 + 2 = 113.
DROP TABLE IF EXISTS t_shadowed_names_form;
CREATE TABLE t_shadowed_names_form (`__shadowed_0.value` UInt64, value UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadowed_names_form VALUES (100, 10);
SELECT value FROM t_shadowed_names_form AS t PREWHERE arrayMap(value -> `__shadowed_0.value` + t.value + value, [toUInt8(1)])[1] = 111 AND `__shadowed_0.value` + 1 = 101;
SELECT value FROM t_shadowed_names_form AS t PREWHERE arrayMap((value, `__shadowed_0.value`) -> toUInt8(t.`__shadowed_0.value`) + t.value + value + `__shadowed_0.value`, [toUInt8(1)], [toUInt8(2)])[1] = 113;
DROP TABLE t_shadowed_names_form;
