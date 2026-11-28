-- A lambda argument may be spelled as the identifier of a table column, here `__table1.value`.
-- The column and the functions of it are still told from the argument and the functions of the argument.

-- An `ALIAS` column with a constant expression is a constant named after the column.
DROP TABLE IF EXISTS t_shadowed_constant_alias;
CREATE TABLE t_shadowed_constant_alias (id UInt8, value UInt64 ALIAS 10) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadowed_constant_alias (id) VALUES (0);

SELECT arrayMap(`__table1.value` -> t.value + `__table1.value`, [toUInt8(1)]) FROM t_shadowed_constant_alias AS t;
SELECT arrayMap(`__table1.value` -> `__table1.value` + t.value, [toUInt8(1)]) FROM t_shadowed_constant_alias AS t;
SELECT arrayMap(`__table1.value` -> (`__table1.value`, t.value), [toUInt8(1)]) FROM t_shadowed_constant_alias AS t;
SELECT arrayMap(`__table1.value` -> (t.value, `__table1.value`), [toUInt8(1)]), t.value FROM t_shadowed_constant_alias AS t;
SELECT arrayMap(`__table1.value` -> arrayMap(`__shadowed_0.__table1.value` -> (`__table1.value`, t.value, `__shadowed_0.__table1.value`), [toUInt8(2)]), [toUInt8(1)])
FROM t_shadowed_constant_alias AS t;

-- In `PREWHERE`, columns are named by their names.
SELECT arrayMap(value -> (value, t.value), [toUInt8(1)]) FROM t_shadowed_constant_alias AS t
PREWHERE arrayExists(value -> value + t.value = 11, [toUInt8(1)]);
SELECT arrayMap(value -> (value, t.value), [toUInt8(1)]) FROM t_shadowed_constant_alias AS t
PREWHERE arrayExists(value -> t.value + value = 11, [toUInt8(1)]);

DROP TABLE t_shadowed_constant_alias;

-- A function of the lambda argument is not the `GROUP BY` key, although both are named `plus(__table1.value, 1_UInt8)`.
DROP TABLE IF EXISTS t_shadowed_group_key;
CREATE TABLE t_shadowed_group_key (value UInt64) ENGINE = Memory;
INSERT INTO t_shadowed_group_key VALUES (10), (20);

SELECT arrayMap(`__table1.value` -> `__table1.value` + 1, [count()])[1] AS k
FROM t_shadowed_group_key AS t GROUP BY t.value + 1 ORDER BY k
SETTINGS optimize_injective_functions_in_group_by = 0;

SELECT arrayMap(`__table1.value` -> `__table1.value` + 1, [count()])[1] AS k, t.value + 1 AS key
FROM t_shadowed_group_key AS t GROUP BY t.value + 1 ORDER BY key
SETTINGS optimize_injective_functions_in_group_by = 0;

-- The key itself is still captured by the lambda: (2, 11), (2, 21).
SELECT arrayMap(`__table1.value` -> (`__table1.value` + 1, t.value + 1), [count()])[1] AS k
FROM t_shadowed_group_key AS t GROUP BY t.value + 1 ORDER BY k
SETTINGS optimize_injective_functions_in_group_by = 0;

-- The argument of the nested lambda is not an argument of an enclosing lambda for the function of the key.
SELECT arrayMap(x -> arrayMap(`__table1.value` -> `__table1.value` + 1, [x])[1] + (t.value + 1), [count()])[1] AS k
FROM t_shadowed_group_key AS t GROUP BY t.value + 1 ORDER BY k
SETTINGS optimize_injective_functions_in_group_by = 0;

DROP TABLE t_shadowed_group_key;
