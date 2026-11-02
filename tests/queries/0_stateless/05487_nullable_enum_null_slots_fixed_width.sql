-- Before a function with the default implementation for NULLs runs on the nested column of a
-- `Nullable(Enum)` argument that holds a NULL, the null slots are set to the enum's default
-- (`patchNullSlots` in src/Functions/IFunction.cpp), because the slot may hold a value the enum does
-- not declare. A fixed-width column is now copied in one pass over the null map instead of with a
-- virtual call per row. The answers must not depend on which path made the copy: for every density
-- of NULLs, for `Enum8` and `Enum16`, for a slot that holds an undeclared value, and on the sibling
-- path that normalizes every `Nullable` argument when the result type cannot be `Nullable`.

DROP TABLE IF EXISTS t_05487;
CREATE TABLE t_05487
(
    id UInt32,
    e8 Nullable(Enum8('a' = 1, 'b' = 2)),
    e16 Nullable(Enum16('a' = 1, 'b' = 2))
)
ENGINE = MergeTree ORDER BY id;

-- 100000 rows: every 1000th row is NULL (0.1 %, the issue's shape), the other even rows are 'a', the odd rows 'b'.
INSERT INTO t_05487 SELECT number, if(number % 1000 = 0, NULL, if(number % 2 = 0, 'a', 'b')), if(number % 1000 = 0, NULL, if(number % 2 = 0, 'a', 'b')) FROM numbers(100000);

SELECT 'rows', count(), countIf(e8 IS NULL), countIf(e16 IS NULL) FROM t_05487;

-- `c = <enum constant>` takes the all-numeric fast path, the other comparisons the general path.
SELECT 'Enum8 = enum constant', count() FROM t_05487 WHERE e8 = CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)');
SELECT 'Enum8 = string', count() FROM t_05487 WHERE e8 = 'a';
SELECT 'Enum8 LIKE', count() FROM t_05487 WHERE e8 LIKE 'a%';
SELECT 'Enum8 match', count() FROM t_05487 WHERE match(e8, '^b');
SELECT 'Enum16 = enum constant', count() FROM t_05487 WHERE e16 = CAST('a', 'Enum16(\'a\' = 1, \'b\' = 2)');
SELECT 'Enum16 LIKE', count() FROM t_05487 WHERE e16 LIKE 'b%';
SELECT 'NULL in, NULL out', countIf((e8 = 'a') IS NULL), countIf((e8 LIKE 'a%') IS NULL), countIf((e16 = 'a') IS NULL) FROM t_05487;

-- Whether the null rows are filtered out before the function runs must not matter.
SELECT 'short circuit off', count() FROM t_05487 WHERE e8 LIKE 'a%' SETTINGS short_circuit_function_evaluation_for_nulls = 0;
SELECT 'short circuit on', count() FROM t_05487 WHERE e8 LIKE 'a%' SETTINGS short_circuit_function_evaluation_for_nulls = 1, short_circuit_function_evaluation_for_nulls_threshold = 0;

-- The rows around a NULL, value by value.
SELECT id, e8, e8 = 'a', e8 LIKE 'a%', e16 = 'b' FROM t_05487 WHERE id < 3 OR id BETWEEN 999 AND 1001 ORDER BY id;

-- Every other row NULL (the shape a LEFT JOIN with join_use_nulls = 1 produces), a single NULL, and nothing but NULLs.
SELECT 'every other row NULL', count() FROM (SELECT CAST(if(number % 2 = 0, NULL, 'a'), 'Nullable(Enum8(\'a\' = 1, \'b\' = 2))') AS e FROM numbers(100000)) WHERE e = 'a';
SELECT 'one NULL', count() FROM (SELECT CAST(if(number = 777, NULL, 'a'), 'Nullable(Enum8(\'a\' = 1, \'b\' = 2))') AS e FROM numbers(100000)) WHERE e = 'a';
SELECT 'all NULL', count(), countIf(e = CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)')), countIf((e = CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)')) IS NULL) FROM (SELECT materialize(CAST(NULL, 'Nullable(Enum8(\'a\' = 1, \'b\' = 2))')) AS e FROM numbers(1000));
-- The default of an enum is its lowest value, whatever the declaration order.
SELECT 'declared out of order', count() FROM (SELECT CAST(if(number % 2 = 0, NULL, 'b'), 'Nullable(Enum8(\'b\' = 2, \'a\' = 1))') AS e FROM numbers(1000)) WHERE e = 'b';

-- `INSERT ... VALUES (NULL)` leaves a raw 0 under the NULL, which this enum does not declare (the
-- String conversion used above stores the enum's default there instead).
DROP TABLE IF EXISTS t_05487_zero_slot;
CREATE TABLE t_05487_zero_slot (e Nullable(Enum8('a' = 1, 'b' = 2))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05487_zero_slot VALUES (NULL), ('a'), ('b'), (NULL);
SELECT 'zero slot', count(), countIf(toInt8(assumeNotNull(e)) = 0) FROM t_05487_zero_slot WHERE e IS NULL;
SELECT 'zero slot, fast path', count() FROM t_05487_zero_slot WHERE e = CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)');
SELECT 'zero slot, general path', count() FROM t_05487_zero_slot WHERE e LIKE 'a%';
SELECT e, e = CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)'), e LIKE 'a%' FROM t_05487_zero_slot ORDER BY e NULLS FIRST;
DROP TABLE t_05487_zero_slot;

-- A function whose result type cannot be `Nullable` runs over the type's default for a NULL row,
-- whatever the slot holds: `nullIf` leaves the compared value in the nested column under the null map.
SELECT 'f(default) for a NULL row', bitmaskToArray(nullIf(materialize(toUInt8(5)), 5)), bitmaskToArray(nullIf(materialize(toInt64(6)), 6)), bitmaskToArray(nullIf(materialize(toUInt8(5)), 4));
SELECT 'mixed rows', groupArray(bitmaskToArray(nullIf(toUInt8(number), 2))), groupArray(bitmaskToArray(nullIf(number, 2))) FROM numbers(4);
SELECT 'DateTime', timeSlots(nullIf(materialize(toDateTime('2020-01-01 00:00:00', 'UTC')), toDateTime('2020-01-01 00:00:00', 'UTC')), toUInt32(600));
SELECT 'DateTime64', timeSlots(nullIf(materialize(toDateTime64('2020-01-01 00:00:00', 3, 'UTC')), toDateTime64('2020-01-01 00:00:00', 3, 'UTC')), toDecimal64(600, 3));
-- A NULL duration runs over the Decimal default 0 at the column's scale.
SELECT 'Decimal64 scale 3', timeSlots(materialize(toDateTime64('2020-01-01 00:10:00', 3, 'UTC')), nullIf(materialize(toDecimal64(600, 3)), toDecimal64(600, 3)));

DROP TABLE t_05487;
