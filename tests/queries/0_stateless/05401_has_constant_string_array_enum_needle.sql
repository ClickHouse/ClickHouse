-- A constant array of strings searched for an `Enum` value matches by the name of the enum value,
-- as `hasAny` and the same search over a non-constant array do.

SELECT 'constant needle';
WITH CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)') AS e
SELECT has(['a', 'c'], e), notHas(['a', 'c'], e), indexOf(['x', 'a'], e), countEqual(['a', 'c', 'a'], e), has(['1', '3'], e), has(['zzz'], e);

SELECT 'indexOfAssumeSorted';
WITH CAST('b', 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)') AS e
SELECT indexOfAssumeSorted(['a', 'b', 'c'], e), indexOfAssumeSorted(['1', '2', '3'], e);

SELECT 'Enum16';
SELECT has(['hello'], CAST('hello', 'Enum16(\'hello\' = -300, \'world\' = 500)')), indexOf(['x', 'world'], CAST('world', 'Enum16(\'hello\' = -300, \'world\' = 500)'));

SELECT 'Nullable';
WITH CAST('a', 'Nullable(Enum8(\'a\' = 1, \'b\' = 2))') AS e
SELECT has(['a', NULL], e), has(CAST(['a'], 'Array(Nullable(String))'), e), has(['a', NULL], CAST(NULL, 'Nullable(Enum8(\'a\' = 1, \'b\' = 2))')), has(['a'], CAST(NULL, 'Nullable(Enum8(\'a\' = 1, \'b\' = 2))'));

SELECT 'FixedString elements';
WITH CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)') AS e
SELECT has([toFixedString('a', 3)], e), indexOf([toFixedString('b', 1), toFixedString('a', 2)], e);

SELECT 'constant map';
WITH CAST('a', 'Enum8(\'a\' = 1, \'b\' = 2)') AS e
SELECT mapContainsKey(map('a', 1), e), mapContains(map('b', 1), e), mapContainsValue(map(1, 'a'), e);

SELECT 'arrayExists rewritten to has';
SELECT arrayExists(x -> x = CAST('a', 'Enum8(\'a\' = 1)'), ['a']) SETTINGS optimize_rewrite_array_exists_to_has = 1;
SELECT arrayExists(x -> x = CAST('a', 'Enum8(\'a\' = 1)'), ['a']) SETTINGS optimize_rewrite_array_exists_to_has = 0;

DROP TABLE IF EXISTS t_enum_needle;
CREATE TABLE t_enum_needle (id UInt32, e Enum8('a' = 1, 'b' = 2), ne Nullable(Enum8('a' = 1, 'b' = 2)), INDEX bf e TYPE bloom_filter GRANULARITY 1)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_enum_needle VALUES (1, 'a', 'a'), (2, 'b', NULL), (3, 'b', 'b');

SELECT 'Enum column';
SELECT id, has(['a'], e), indexOf(['x', 'b'], e), has(['a', NULL], ne), has(CAST(['a'], 'Array(LowCardinality(String))'), e) FROM t_enum_needle ORDER BY id;

SELECT 'bloom_filter index';
SELECT count() FROM t_enum_needle WHERE has(['a'], e) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_enum_needle WHERE has(['a'], e) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_enum_needle WHERE has(['zzz'], e) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_enum_needle WHERE has(['zzz'], e) SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_enum_needle WHERE has([toFixedString('b', 2)], e) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_enum_needle WHERE has([toFixedString('b', 2)], e) SETTINGS use_skip_indexes = 0;
-- The index is used and keeps only the granules that hold the name.
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_enum_needle WHERE has(['a'], e)) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';
SELECT replaceRegexpOne(explain, '^[^A-Za-z]*', '') FROM (EXPLAIN indexes = 1 SELECT id FROM t_enum_needle WHERE has([toFixedString('b', 2)], e)) WHERE explain LIKE '%Name:%' OR explain LIKE '%Granules:%';

DROP TABLE t_enum_needle;
