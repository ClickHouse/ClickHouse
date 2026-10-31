-- `cuckoo_filter` and `binary_fuse_filter` must not change the result of a query, the same as `bloom_filter`:
-- * `0.0` and `-0.0` are equal, but hash apart;
-- * a `String` constant searched in `Array(Enum)` or in `Map` keys of `Enum` is compared by the name of the
--   enum value, and a name that is not in the `Enum` matches nothing instead of throwing;
-- * a map key that is not in the `Enum` key type throws in `arrayElement`, which the index must not hide;
-- * for a missing key `arrayElement` over `Enum` values returns the zero of the underlying integer.

SET allow_experimental_cuckoo_filter_index = 1;
SET allow_experimental_binary_fuse_filter_index = 1;
SET optimize_functions_to_subcolumns = 0;

SELECT 'cuckoo_filter';

DROP TABLE IF EXISTS t_zero;
CREATE TABLE t_zero (id UInt64, x Float64, arr Array(Float32), INDEX idx_x x TYPE cuckoo_filter GRANULARITY 1, INDEX idx_arr arr TYPE cuckoo_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_zero VALUES (1, 0.0, [0.0]), (2, -0.0, [-0.0]), (3, 1.5, [1.5]);
SELECT groupArray(id) FROM (SELECT id FROM t_zero WHERE x = -0.0 ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_zero WHERE x = 0.0 ORDER BY id);
-- A set compares floats bitwise, so only `-0.0` is in it; the index must agree.
SELECT groupArray(id) FROM (SELECT id FROM t_zero WHERE x IN (-0.0, 7) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_zero WHERE has(arr, toFloat32(-0.0)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_zero WHERE hasAll(arr, [toFloat32(0.0)]) ORDER BY id);
DROP TABLE t_zero;

DROP TABLE IF EXISTS t_enum_arr;
CREATE TABLE t_enum_arr
(
    id UInt64,
    arr Array(Enum8('a' = 1, 'b' = 2, 'c' = 3)),
    m Map(Enum8('a' = 1, 'b' = 2, 'c' = 3), UInt64),
    INDEX idx_arr arr TYPE cuckoo_filter GRANULARITY 1,
    INDEX idx_m mapKeys(m) TYPE cuckoo_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_enum_arr VALUES (1, ['a'], {'a': 1}), (2, ['b'], {'b': 2}), (3, ['c'], {'c': 3});
SELECT groupArray(id) FROM (SELECT id FROM t_enum_arr WHERE has(arr, toFixedString('a', 2)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_enum_arr WHERE hasAny(arr, ['b', 'z']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_enum_arr WHERE mapContains(m, toFixedString('c', 2)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_enum_arr WHERE has(arr, 'z') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_enum_arr WHERE hasAny(arr, ['z']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_enum_arr WHERE mapContains(m, 'z') ORDER BY id);
DROP TABLE t_enum_arr;

DROP TABLE IF EXISTS t_map_enum_key;
CREATE TABLE t_map_enum_key
(
    id UInt64,
    m Map(Enum8('a' = 1, 'b' = 2, 'c' = 3), Int64),
    INDEX idx_values mapValues(m) TYPE cuckoo_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_map_enum_key VALUES (1, map('a', 10, 'c', 30)), (2, map('b', 20));
SELECT id FROM t_map_enum_key WHERE m['nonexistent'] = 999 ORDER BY id; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT id FROM t_map_enum_key WHERE m['nonexistent'] IN (999) ORDER BY id; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT groupArray(id) FROM (SELECT id FROM t_map_enum_key WHERE m['b'] = 20 ORDER BY id);
DROP TABLE t_map_enum_key;

DROP TABLE IF EXISTS t_map_enum_values;
CREATE TABLE t_map_enum_values
(
    id UInt64,
    m Map(String, Enum8('m' = -1, 'z' = 0, 'a' = 1)),
    INDEX idx_values mapValues(m) TYPE cuckoo_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_map_enum_values VALUES (1, map('x', 'a')), (2, map('k', 'a')), (3, map('k', 'm'));
SELECT groupArray(id) FROM (SELECT id FROM t_map_enum_values WHERE m['k'] IN ('z') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_map_enum_values WHERE m['k'] IN ('z', 'm') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_map_enum_values WHERE m['k'] = 'z' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_map_enum_values WHERE m['k'] = 'a' ORDER BY id);
DROP TABLE t_map_enum_values;

SELECT 'binary_fuse_filter';

DROP TABLE IF EXISTS t_zero;
CREATE TABLE t_zero (id UInt64, x Float64, arr Array(Float32), INDEX idx_x x TYPE binary_fuse_filter GRANULARITY 1, INDEX idx_arr arr TYPE binary_fuse_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_zero VALUES (1, 0.0, [0.0]), (2, -0.0, [-0.0]), (3, 1.5, [1.5]);
SELECT groupArray(id) FROM (SELECT id FROM t_zero WHERE x = -0.0 ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_zero WHERE x = 0.0 ORDER BY id);
-- A set compares floats bitwise, so only `-0.0` is in it; the index must agree.
SELECT groupArray(id) FROM (SELECT id FROM t_zero WHERE x IN (-0.0, 7) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_zero WHERE has(arr, toFloat32(-0.0)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_zero WHERE hasAll(arr, [toFloat32(0.0)]) ORDER BY id);
DROP TABLE t_zero;

DROP TABLE IF EXISTS t_enum_arr;
CREATE TABLE t_enum_arr
(
    id UInt64,
    arr Array(Enum8('a' = 1, 'b' = 2, 'c' = 3)),
    m Map(Enum8('a' = 1, 'b' = 2, 'c' = 3), UInt64),
    INDEX idx_arr arr TYPE binary_fuse_filter GRANULARITY 1,
    INDEX idx_m mapKeys(m) TYPE binary_fuse_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_enum_arr VALUES (1, ['a'], {'a': 1}), (2, ['b'], {'b': 2}), (3, ['c'], {'c': 3});
SELECT groupArray(id) FROM (SELECT id FROM t_enum_arr WHERE has(arr, toFixedString('a', 2)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_enum_arr WHERE hasAny(arr, ['b', 'z']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_enum_arr WHERE mapContains(m, toFixedString('c', 2)) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_enum_arr WHERE has(arr, 'z') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_enum_arr WHERE hasAny(arr, ['z']) ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_enum_arr WHERE mapContains(m, 'z') ORDER BY id);
DROP TABLE t_enum_arr;

DROP TABLE IF EXISTS t_map_enum_key;
CREATE TABLE t_map_enum_key
(
    id UInt64,
    m Map(Enum8('a' = 1, 'b' = 2, 'c' = 3), Int64),
    INDEX idx_values mapValues(m) TYPE binary_fuse_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_map_enum_key VALUES (1, map('a', 10, 'c', 30)), (2, map('b', 20));
SELECT id FROM t_map_enum_key WHERE m['nonexistent'] = 999 ORDER BY id; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT id FROM t_map_enum_key WHERE m['nonexistent'] IN (999) ORDER BY id; -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT groupArray(id) FROM (SELECT id FROM t_map_enum_key WHERE m['b'] = 20 ORDER BY id);
DROP TABLE t_map_enum_key;

DROP TABLE IF EXISTS t_map_enum_values;
CREATE TABLE t_map_enum_values
(
    id UInt64,
    m Map(String, Enum8('m' = -1, 'z' = 0, 'a' = 1)),
    INDEX idx_values mapValues(m) TYPE binary_fuse_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_map_enum_values VALUES (1, map('x', 'a')), (2, map('k', 'a')), (3, map('k', 'm'));
SELECT groupArray(id) FROM (SELECT id FROM t_map_enum_values WHERE m['k'] IN ('z') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_map_enum_values WHERE m['k'] IN ('z', 'm') ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_map_enum_values WHERE m['k'] = 'z' ORDER BY id);
SELECT groupArray(id) FROM (SELECT id FROM t_map_enum_values WHERE m['k'] = 'a' ORDER BY id);
DROP TABLE t_map_enum_values;
