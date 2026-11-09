-- An `Enum` is compared with a `FixedString` by its name, without the trailing zero bytes of the `FixedString`, so
-- `toFixedString('a', 1)` equals 'a' but not 'a\0'. Index analysis and the replacement of a filtered column with a constant agree.

DROP TABLE IF EXISTS t_enum_bf;
CREATE TABLE t_enum_bf
(
    id UInt64,
    e Enum8('z' = 0, 'a' = 1, 'a\0' = 2),
    m Map(String, Enum8('z' = 0, 'a' = 1, 'a\0' = 2)),
    INDEX idx e TYPE bloom_filter GRANULARITY 1,
    INDEX idx_m mapValues(m) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_enum_bf VALUES (1, 'a', {'k': 'a'}), (2, 'a\0', {'k': 'a\0'});

SELECT 'bloom_filter';
SELECT id FROM t_enum_bf WHERE e = toFixedString('a', 1) ORDER BY id SETTINGS use_skip_indexes = 0;
SELECT id FROM t_enum_bf WHERE e = toFixedString('a', 1) ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT id FROM t_enum_bf WHERE m['k'] = toFixedString('a', 1) ORDER BY id SETTINGS use_skip_indexes = 1;
DROP TABLE t_enum_bf;

SELECT 'partition and primary key';
DROP TABLE IF EXISTS t_enum_part;
CREATE TABLE t_enum_part (e Enum8('z' = 0, 'a' = 1, 'a\0' = 2))
ENGINE = MergeTree PARTITION BY e ORDER BY e;
INSERT INTO t_enum_part VALUES ('a');
INSERT INTO t_enum_part VALUES ('a\0');
SELECT count() FROM t_enum_part WHERE e = toFixedString('a', 1);
SELECT count() FROM t_enum_part WHERE e = toFixedString('a', 1) SETTINGS optimize_trivial_count_query = 0, optimize_use_implicit_projections = 0;
DROP TABLE t_enum_part;

SELECT 'constant after filter';
DROP TABLE IF EXISTS t_enum_mem;
CREATE TABLE t_enum_mem (e Enum8('z' = 0, 'a' = 1, 'a\0' = 2)) ENGINE = Memory;
INSERT INTO t_enum_mem VALUES ('a'), ('a\0');
SELECT arraySort(groupArray(hex(toString(e)))) FROM t_enum_mem WHERE e = toFixedString('a', 1) SETTINGS optimize_constant_columns_after_filter = 1;
SELECT arraySort(groupArray(hex(toString(e)))) FROM t_enum_mem WHERE e = toFixedString('a', 1) SETTINGS optimize_constant_columns_after_filter = 0;
DROP TABLE t_enum_mem;

-- The query condition cache distinguishes the conversion modes of an aggregate run by `arrayReduceInRanges`
-- over a `Dynamic` argument, which hashes the function resolver.
SELECT 'Dynamic';
DROP TABLE IF EXISTS t_dyn_reduce;
CREATE TABLE t_dyn_reduce (id UInt64, a Dynamic) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dyn_reduce SELECT number, [toFixedString('a', 2)] FROM numbers(1000);

SELECT count() FROM t_dyn_reduce WHERE arrayReduceInRanges('groupConcat', [(1, 1)], a)[1] = 'a' SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT count() FROM t_dyn_reduce WHERE arrayReduceInRanges('groupConcat', [(1, 1)], a)[1] = 'a' SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 1;
DROP TABLE t_dyn_reduce;
