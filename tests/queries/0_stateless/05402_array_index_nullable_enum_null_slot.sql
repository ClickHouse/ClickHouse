-- Under a NULL, a `Nullable(Enum)` column usually holds 0, which this enum does not declare.
-- `has` and the functions built on it compare an enum with a string by name, and must not
-- read the value under a NULL to do that.

DROP TABLE IF EXISTS t_enum_null_slots;
CREATE TABLE t_enum_null_slots
(
    id UInt8,
    a Array(Nullable(Enum8('a' = 1, 'b' = 2))),
    a16 Array(Nullable(Enum16('a' = 1, 'b' = 2))),
    t Array(Nullable(Tuple(Enum8('a' = 1, 'b' = 2)))),
    m Map(String, Nullable(Enum8('a' = 1, 'b' = 2))),
    s Array(String),
    lc Array(LowCardinality(String)),
    mk Map(String, UInt8),
    e Nullable(Enum8('a' = 1, 'b' = 2)),
    ns Nullable(String)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_enum_null_slots VALUES
    (1, ['a', NULL], ['a', NULL], [NULL], {'x': NULL}, ['a'], ['a'], {'a': 1}, NULL, NULL),
    (2, ['b', NULL], ['b', NULL], [('b')], {'y': 'b'}, ['b'], ['b'], {'b': 1}, 'b', 'b'),
    (3, [NULL], [NULL], [NULL, ('a')], {'z': NULL, 'w': 'a'}, ['a'], ['a'], {'a': 1}, 'a', 'a'),
    (4, [], [], [], {}, [], [], {}, NULL, 'a');

SELECT 'placeholder', id, arrayMap(x -> toInt8(assumeNotNull(x)), a), toInt8(assumeNotNull(e)) FROM t_enum_null_slots ORDER BY id;

SELECT 'string', id, has(a, 'b'), notHas(a, 'b'), indexOf(a, 'b'), countEqual(a, 'b'), indexOfAssumeSorted(a, 'b') FROM t_enum_null_slots ORDER BY id;
SELECT 'first value', id, has(a, 'a'), indexOf(a, 'a'), countEqual(a, 'a') FROM t_enum_null_slots ORDER BY id;
SELECT 'FixedString', id, has(a, toFixedString('b', 1)) FROM t_enum_null_slots ORDER BY id;
SELECT 'Enum16', id, has(a16, 'b'), indexOf(a16, 'a') FROM t_enum_null_slots ORDER BY id;
SELECT 'Tuple', id, has(t, tuple('b')), has(t, tuple('a')) FROM t_enum_null_slots ORDER BY id;
SELECT 'Nullable(String) needle', id, has(a, ns), indexOf(a, ns), countEqual(a, ns) FROM t_enum_null_slots ORDER BY id;
SELECT 'Nullable(Enum) needle', id, has(s, e), indexOf(s, e), countEqual(s, e), has(lc, e), mapContainsKey(mk, e) FROM t_enum_null_slots ORDER BY id;
SELECT 'mapContainsValue', id, mapContainsValue(m, 'b'), mapContainsValue(m, 'a') FROM t_enum_null_slots ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT 'mapContainsValue subcolumn', id, mapContainsValue(m, 'b'), mapContainsValue(m, 'a') FROM t_enum_null_slots ORDER BY id SETTINGS optimize_functions_to_subcolumns = 1;
SELECT 'arrayExists', id, arrayExists(x -> x = 'b', a) FROM t_enum_null_slots ORDER BY id SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT 'IN', id, 'b' IN a, 'a' IN a FROM t_enum_null_slots ORDER BY id;

-- The padding of the non-joined rows of a `LEFT JOIN` is the other producer of such a slot.
SELECT 'join', l.id, has(l.s, r.e)
FROM t_enum_null_slots AS l
LEFT JOIN (SELECT 3 AS id, CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)') AS e) AS r ON l.id = r.id
ORDER BY l.id
SETTINGS join_use_nulls = 1, join_algorithm = 'hash';

DROP TABLE t_enum_null_slots;
