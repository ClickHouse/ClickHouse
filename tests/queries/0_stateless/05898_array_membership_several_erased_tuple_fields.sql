-- A `Tuple` with several type-erasing fields: rows are grouped by the alternatives they carry at
-- every erased position, not only at the first one. A row that differs from its neighbours in a
-- later field must not drag them back to the comparison that orders by the variant name.
-- Every row puts the function result next to the element-wise `=` oracle. The oracle compares a
-- whole block at once, so it needs the mismatching alternatives to yield NULL instead of an exception;
-- the queries with one row per block run with the default settings.

SET optimize_rewrite_array_exists_to_has = 0;

DROP TABLE IF EXISTS t_elements;
DROP TABLE IF EXISTS t_needle;
DROP TABLE IF EXISTS t_three;
DROP TABLE IF EXISTS t_nested;

SELECT 'erased elements, rows differ in the second field';
CREATE TABLE t_elements (id UInt8, a Array(Tuple(Dynamic, Dynamic))) ENGINE = Memory;
INSERT INTO t_elements VALUES (0, [(1::UInt64, 2::UInt64)]), (1, [(1::UInt64, 'x')]), (2, [(1::UInt64, 3::UInt64), (1::UInt64, 2::UInt64)]);
SELECT id,
    has(a, (1::UInt8, 2::UInt8)), arrayExists(e -> e = (1::UInt8, 2::UInt8), a),
    indexOf(a, (1::UInt8, 2::UInt8)), arrayFirstIndex(e -> e = (1::UInt8, 2::UInt8), a),
    countEqual(a, (1::UInt8, 2::UInt8)), arrayCount(e -> e = (1::UInt8, 2::UInt8), a)
FROM t_elements ORDER BY id SETTINGS dynamic_throw_on_type_mismatch = 0, variant_throw_on_type_mismatch = 0;
SELECT 'the same, one row per block';
SELECT id, has(a, (1::UInt8, 2::UInt8)) FROM t_elements ORDER BY id SETTINGS max_block_size = 1;

SELECT 'erased needle, rows differ in the second field';
CREATE TABLE t_needle (id UInt8, v Array(Tuple(UInt64, UInt64)), n Tuple(Dynamic, Dynamic)) ENGINE = Memory;
INSERT INTO t_needle VALUES (0, [(1, 2)], (1::UInt8, 2::UInt8)), (1, [(1, 3)], (1::UInt8, 'x')), (2, [(5, 5), (1, 7)], (1::UInt8, 7::UInt16));
SELECT id,
    has(v, n), arrayExists(e -> e = n, v),
    indexOf(v, n), arrayFirstIndex(e -> e = n, v),
    countEqual(v, n), arrayCount(e -> e = n, v)
FROM t_needle ORDER BY id SETTINGS dynamic_throw_on_type_mismatch = 0, variant_throw_on_type_mismatch = 0;
SELECT 'the same, one row per block';
SELECT id, has(v, n) FROM t_needle ORDER BY id SETTINGS max_block_size = 1;

SELECT 'three erased fields, rows differ in the last one';
CREATE TABLE t_three (id UInt8, a Array(Tuple(Dynamic, Dynamic, Dynamic))) ENGINE = Memory;
INSERT INTO t_three VALUES (0, [(1::UInt64, 2::UInt64, 3::UInt64)]), (1, [(1::UInt64, 2::UInt64, 'x')]), (2, [(1::UInt64, 2::UInt64, 3::Int32)]);
SELECT id,
    has(a, (1::UInt8, 2::UInt8, 3::UInt8)), arrayExists(e -> e = (1::UInt8, 2::UInt8, 3::UInt8), a)
FROM t_three ORDER BY id SETTINGS dynamic_throw_on_type_mismatch = 0, variant_throw_on_type_mismatch = 0;

SELECT 'erased fields at different depths';
CREATE TABLE t_nested (id UInt8, a Array(Tuple(Tuple(Dynamic, UInt8), Dynamic))) ENGINE = Memory;
INSERT INTO t_nested VALUES (0, [((1::UInt64, 0), 2::UInt64)]), (1, [((1::UInt64, 0), 'x')]), (2, [(('y', 0), 2::UInt64)]);
SELECT id,
    has(a, ((1::UInt8, 0::UInt8), 2::UInt8)), arrayExists(e -> e = ((1::UInt8, 0::UInt8), 2::UInt8), a)
FROM t_nested ORDER BY id SETTINGS dynamic_throw_on_type_mismatch = 0, variant_throw_on_type_mismatch = 0;

DROP TABLE t_elements;
DROP TABLE t_needle;
DROP TABLE t_three;
DROP TABLE t_nested;
