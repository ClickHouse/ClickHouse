-- Tags: no-random-merge-tree-settings
-- Regression tests for `map_serialization_version = 'with_key_columns'`.

SET allow_experimental_map_key_columns = 1;
SET enable_analyzer = 1;

SELECT '-- zero-level only';
DROP TABLE IF EXISTS t_kc_zero;
CREATE TABLE t_kc_zero (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id SETTINGS map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }
CREATE TABLE t_kc_zero (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_kc_zero MODIFY SETTING map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }
DROP TABLE t_kc_zero;

SELECT '-- value stream collision';
CREATE TABLE t_kc_collision (id UInt64, m Map(String, Tuple(`a.b` UInt64, a Tuple(b UInt64)))) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_bytes_for_wide_part = 0; -- { serverError BAD_ARGUMENTS }

SELECT '-- source column check';
DROP TABLE IF EXISTS t_kc_source;
CREATE TABLE t_kc_source (id UInt64, j Map(String, UInt64), `j.key_a` Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', min_bytes_for_wide_part = 0, share_nested_offsets = 0;
INSERT INTO t_kc_source VALUES (1, {'a.key_k':17}, {'k':42});
SELECT `j.key_a`['k'], mapContains(`j.key_a`, 'k'), mapKeys(`j.key_a`) FROM t_kc_source SETTINGS optimize_functions_to_subcolumns = 1;
SELECT `j.key_a`['k'], mapContains(`j.key_a`, 'k'), mapKeys(`j.key_a`) FROM t_kc_source SETTINGS optimize_functions_to_subcolumns = 0;
DROP TABLE t_kc_source;

SELECT '-- tuple map default';
DROP TABLE IF EXISTS t_kc_tuple_default;
CREATE TABLE t_kc_tuple_default (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', min_bytes_for_wide_part = 0, serialization_info_version = 'with_types', propagate_types_serialization_versions_to_nested_types = 1;
INSERT INTO t_kc_tuple_default VALUES (7);
ALTER TABLE t_kc_tuple_default ADD COLUMN x Tuple(m Map(String, UInt64)) DEFAULT tuple(map('a', id));
ALTER TABLE t_kc_tuple_default ADD COLUMN y Tuple(m Map(String, UInt64), s UInt64) DEFAULT tuple(map('b', id), id);
SELECT x, y FROM t_kc_tuple_default SETTINGS use_columns_cache = 0;
DROP TABLE t_kc_tuple_default;

SELECT '-- old basic nested map';
DROP TABLE IF EXISTS t_kc_nested_basic;
CREATE TABLE t_kc_nested_basic (id UInt64, a Array(Map(String, UInt64))) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', min_bytes_for_wide_part = 0, index_granularity = 4, merge_max_block_size = 4,
    vertical_merge_algorithm_min_rows_to_activate = 1000000, propagate_types_serialization_versions_to_nested_types = 0;
INSERT INTO t_kc_nested_basic SELECT number, [if(number < 4, map('a', number), map('b', number))] FROM numbers(20);
ALTER TABLE t_kc_nested_basic MODIFY SETTING propagate_types_serialization_versions_to_nested_types = 1;
OPTIMIZE TABLE t_kc_nested_basic FINAL;
SELECT count(), sum(length(a[1])), arraySort(groupUniqArrayArray(mapKeys(a[1]))) FROM t_kc_nested_basic;
DROP TABLE t_kc_nested_basic;

SELECT '-- mutations';
DROP TABLE IF EXISTS t_kc_mutations;
CREATE TABLE t_kc_mutations (id UInt64, x UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_kc_mutations;
INSERT INTO t_kc_mutations VALUES (1, 0, {'q':1});
ALTER TABLE t_kc_mutations UPDATE m = map('z', id, 'a', id) WHERE 1;
SELECT 'on the fly', m, mapKeys(m)[1] FROM t_kc_mutations SETTINGS apply_mutations_on_fly = 1;
ALTER TABLE t_kc_mutations UPDATE x = 1 WHERE mapKeys(m)[1] = 'a';
SYSTEM START MERGES t_kc_mutations;
ALTER TABLE t_kc_mutations UPDATE x = x WHERE 0 SETTINGS mutations_sync = 2;
SELECT 'coalesced', m, x FROM t_kc_mutations;
DROP TABLE t_kc_mutations;
