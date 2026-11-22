-- Aggregate functions of a `String` argument with dictionary granules equal those of its `with_size_stream`
-- twin. With keys, a function reads the argument through its indexes, and one that ignores repeated values
-- skips the rows that repeat a position and a place; a `UInt8` key keeps them off the lookup table of its
-- method. Without keys, a function reads the argument through its indexes or, when it ignores repeated
-- values, takes each used value once. Two `String` keys, one of them `Nullable`, go through serialized
-- methods.

DROP TABLE IF EXISTS t_agg_plain;
DROP TABLE IF EXISTS t_agg_dict;

CREATE TABLE t_agg_plain (id UInt64, k UInt8, s String, s2 String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream';

CREATE TABLE t_agg_dict (id UInt64, k UInt8, s String, s2 String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, serialization_info_version = 'with_types', string_serialization_version = 'with_dictionaries';

INSERT INTO t_agg_plain SELECT number, number % 10, 'v' || toString(cityHash64(number) % 50), 'w' || toString(number % 13) FROM numbers(20000);
INSERT INTO t_agg_dict SELECT number, number % 10, 'v' || toString(cityHash64(number) % 50), 'w' || toString(number % 13) FROM numbers(20000);

SELECT 'UInt8 key', (SELECT groupArray(tuple(*)) FROM (SELECT k, cityHash64(arraySort(groupArray(s))), min(s), max(s), uniqUpTo(30)(s), uniqCombined(s) FROM t_agg_plain GROUP BY k ORDER BY k))
    = (SELECT groupArray(tuple(*)) FROM (SELECT k, cityHash64(arraySort(groupArray(s))), min(s), max(s), uniqUpTo(30)(s), uniqCombined(s) FROM t_agg_dict GROUP BY k ORDER BY k));
SELECT 'UInt64 key', (SELECT groupArray(tuple(*)) FROM (SELECT id % 100 AS key, cityHash64(arraySort(groupArray(s))), min(s), max(s), uniqUpTo(30)(s) FROM t_agg_plain GROUP BY key ORDER BY key))
    = (SELECT groupArray(tuple(*)) FROM (SELECT id % 100 AS key, cityHash64(arraySort(groupArray(s))), min(s), max(s), uniqUpTo(30)(s) FROM t_agg_dict GROUP BY key ORDER BY key));
SELECT 'without keys', (SELECT tuple(cityHash64(arraySort(groupArray(s))), min(s), max(s), uniqUpTo(30)(s), uniqCombined(s), uniqCombined64(s)) FROM t_agg_plain)
    = (SELECT tuple(cityHash64(arraySort(groupArray(s))), min(s), max(s), uniqUpTo(30)(s), uniqCombined(s), uniqCombined64(s)) FROM t_agg_dict);
SELECT 'two String keys', (SELECT groupArray(tuple(*)) FROM (SELECT s, s2, count() FROM t_agg_plain GROUP BY s, s2 ORDER BY s, s2))
    = (SELECT groupArray(tuple(*)) FROM (SELECT s, s2, count() FROM t_agg_dict GROUP BY s, s2 ORDER BY s, s2));
SELECT 'two String keys, one Nullable', (SELECT groupArray(tuple(*)) FROM (SELECT s, toNullable(s2) AS n, count() FROM t_agg_plain GROUP BY s, n ORDER BY s, n))
    = (SELECT groupArray(tuple(*)) FROM (SELECT s, toNullable(s2) AS n, count() FROM t_agg_dict GROUP BY s, n ORDER BY s, n));

DROP TABLE t_agg_plain;
DROP TABLE t_agg_dict;
