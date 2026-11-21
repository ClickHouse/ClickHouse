-- The query condition cache must tell apart the `cast_fixed_string_to_string_strip_trailing_zeros` modes
-- of a function wrapped by the `Dynamic` or `Variant` adaptor.

DROP TABLE IF EXISTS t_fs_cache_dynamic;
DROP TABLE IF EXISTS t_fs_cache_variant;

CREATE TABLE t_fs_cache_dynamic (id UInt64, k Dynamic) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_fs_cache_dynamic SELECT number, [toFixedString('a', 2)] FROM numbers(20000);

SELECT count() FROM t_fs_cache_dynamic WHERE arrayStringConcat(k) = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT count() FROM t_fs_cache_dynamic WHERE arrayStringConcat(k) = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 1;
SELECT count() FROM t_fs_cache_dynamic WHERE arrayStringConcat(k) = 'a'
SETTINGS use_query_condition_cache = 0, cast_fixed_string_to_string_strip_trailing_zeros = 1;

CREATE TABLE t_fs_cache_variant (id UInt64, k Variant(Array(FixedString(2)), UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_fs_cache_variant SELECT number, [toFixedString('a', 2)]::Array(FixedString(2)) FROM numbers(20000);

SELECT count() FROM t_fs_cache_variant WHERE arrayStringConcat(k) = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 0;
SELECT count() FROM t_fs_cache_variant WHERE arrayStringConcat(k) = 'a'
SETTINGS use_query_condition_cache = 1, cast_fixed_string_to_string_strip_trailing_zeros = 1;
SELECT count() FROM t_fs_cache_variant WHERE arrayStringConcat(k) = 'a'
SETTINGS use_query_condition_cache = 0, cast_fixed_string_to_string_strip_trailing_zeros = 1;

DROP TABLE t_fs_cache_dynamic;
DROP TABLE t_fs_cache_variant;
