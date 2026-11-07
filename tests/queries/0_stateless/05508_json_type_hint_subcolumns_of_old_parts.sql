-- Tags: long
-- long: many statements; over the time limit of the flaky check, which runs its copies in parallel.

-- Subcolumns of a JSON column read from a part that still stores an older type of the column (a type hint changed by
-- a lazy `MODIFY COLUMN`, or a `MODIFY COLUMN` mutation that has not rewritten the part yet) must be the subcolumns of
-- the converted column: the same values as after the part is rewritten.

SET optimize_functions_to_subcolumns = 1;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_issue;
DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_omitted;
DROP TABLE IF EXISTS t_pending;
DROP TABLE IF EXISTS t_to_string;
DROP TABLE IF EXISTS t_mutation;
DROP TABLE IF EXISTS t_array;
DROP TABLE IF EXISTS t_single_stream;
DROP TABLE IF EXISTS t_reverse;
DROP TABLE IF EXISTS t_map_to_string;
DROP TABLE IF EXISTS t_add_hint;
DROP TABLE IF EXISTS t_typed_ancestor;
DROP TABLE IF EXISTS t_skip;
DROP TABLE IF EXISTS t_dynamic_hint;
DROP TABLE IF EXISTS t_variant_hint;
DROP TABLE IF EXISTS t_variant_in_part;
DROP TABLE IF EXISTS t_patch_before;
DROP TABLE IF EXISTS t_patch_merged;
DROP TABLE IF EXISTS t_patch_after;
DROP TABLE IF EXISTS t_patch_after_mutation;
DROP TABLE IF EXISTS t_old_patch_after_mutation;
DROP TABLE IF EXISTS t_cast_path;
DROP TABLE IF EXISTS t_cast_path_patch;
DROP TABLE IF EXISTS t_array_map;
DROP TABLE IF EXISTS t_array_tuple;
DROP TABLE IF EXISTS t_nested_wide;
DROP TABLE IF EXISTS t_nested_compact;
DROP TABLE IF EXISTS t_nested_patch;
DROP TABLE IF EXISTS t_parent_prewhere;
DROP TABLE IF EXISTS t_nullable_prewhere;
DROP TABLE IF EXISTS t_omitted_patch_before;
DROP TABLE IF EXISTS t_omitted_patch_after;
DROP TABLE IF EXISTS t_absent_patch;
DROP TABLE IF EXISTS t_absent_patch_map;
DROP TABLE IF EXISTS t_absent_default_patch;
DROP TABLE IF EXISTS t_absent_default_patch_map;
DROP TABLE IF EXISTS t_text_index_typed;
DROP TABLE IF EXISTS t_text_index_map;
DROP TABLE IF EXISTS t_read_cast_path_added;
DROP TABLE IF EXISTS t_read_cast_path_changed;
DROP TABLE IF EXISTS t_read_cast_path_array;
DROP TABLE IF EXISTS t_read_cast_path_tuple;
DROP TABLE IF EXISTS t_scalar_sibling;
DROP TABLE IF EXISTS t_read_cast_path_map;
DROP TABLE IF EXISTS t_scalar_sibling_map;
DROP TABLE IF EXISTS t_default_dep;
DROP TABLE IF EXISTS t_mutation_source_wide;
DROP TABLE IF EXISTS t_mutation_source_compact;
DROP TABLE IF EXISTS t_mutation_source_patch_wide;
DROP TABLE IF EXISTS t_mutation_source_patch_compact;
DROP TABLE IF EXISTS t_chained_mutations;
DROP TABLE IF EXISTS t_chained_mutations_old_patch_wide;
DROP TABLE IF EXISTS t_chained_mutations_old_patch_compact;
DROP TABLE IF EXISTS t_shadowed_path_wide;
DROP TABLE IF EXISTS t_shadowed_path_compact;
DROP TABLE IF EXISTS t_scalar_sibling_nullable_wide;
DROP TABLE IF EXISTS t_scalar_sibling_nullable_compact;

SELECT 'issue 123339';
CREATE TABLE t_issue (id UInt32, j JSON) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '1G';
SYSTEM STOP MERGES t_issue;
INSERT INTO t_issue VALUES (1, '{"a": [1, 2, 3], "m": {"k1": 1, "k2": 2}}'), (2, '{"a": [4], "n": 5, "m": {"k3": 3}}'), (3, '{}');
ALTER TABLE t_issue MODIFY COLUMN j JSON(a Array(UInt32), n Nullable(UInt32), m Map(String, UInt32)) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, j.a.size0, j.n.null, j.m FROM t_issue ORDER BY id;
SELECT count() FROM t_issue WHERE j.n IS NULL;
SELECT sum(length(j.a)) FROM t_issue;
SYSTEM START MERGES t_issue;
OPTIMIZE TABLE t_issue FINAL;
SELECT id, j.a.size0, j.n.null, j.m FROM t_issue ORDER BY id;
SELECT count() FROM t_issue WHERE j.n IS NULL;
SELECT sum(length(j.a)) FROM t_issue;

SELECT 'wide';
CREATE TABLE t_wide (id UInt64, data JSON(a UInt32, b Array(UInt32))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_wide;
INSERT INTO t_wide VALUES (1, '{"a" : 42, "b" : [1, 2]}'), (2, '{"a" : 12345, "b" : []}');
ALTER TABLE t_wide MODIFY COLUMN data JSON(a String, b Array(UInt32)) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, length(data.a), data.a.size, empty(data.a), data.b.size0 FROM t_wide ORDER BY id;
SELECT id, data.a, data.a.:String FROM t_wide ORDER BY id;
SELECT count() FROM t_wide WHERE length(data.a) = 2 SETTINGS optimize_move_to_prewhere = 1;
SELECT count() FROM t_wide WHERE length(data.a) = 2 SETTINGS optimize_move_to_prewhere = 0;
SELECT id, length(data.a) FROM t_wide ORDER BY id LIMIT 1
SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10;

SELECT 'compact';
CREATE TABLE t_compact (id UInt64, data JSON(a UInt32, b Array(UInt32))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '1G';
SYSTEM STOP MERGES t_compact;
INSERT INTO t_compact VALUES (1, '{"a" : 42, "b" : [1, 2]}'), (2, '{"a" : 12345, "b" : []}');
ALTER TABLE t_compact MODIFY COLUMN data JSON(a String, b Array(UInt32)) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, length(data.a), data.a.size, empty(data.a), data.b.size0 FROM t_compact ORDER BY id;
SELECT id, data.a, data.a.:String FROM t_compact ORDER BY id;
SELECT count() FROM t_compact WHERE length(data.a) = 2 SETTINGS optimize_move_to_prewhere = 1;
SELECT count() FROM t_compact WHERE length(data.a) = 2 SETTINGS optimize_move_to_prewhere = 0;

SELECT 'omitted';
CREATE TABLE t_omitted (id UInt64, data JSON(a UInt32)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
SYSTEM STOP MERGES t_omitted;
INSERT INTO t_omitted VALUES (1, '{}'), (2, '{}');
ALTER TABLE t_omitted MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, length(data.a), empty(data.a) FROM t_omitted ORDER BY id;
SELECT id, data.a, data FROM t_omitted ORDER BY id;

SELECT 'pending modify';
CREATE TABLE t_pending (id UInt64, data JSON(a UInt32)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_pending;
INSERT INTO t_pending VALUES (1, '{"a" : 42}'), (2, '{"a" : 12345}');
ALTER TABLE t_pending MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 0, mutations_sync = 0, alter_sync = 0;
SELECT id, length(data.a) FROM t_pending ORDER BY id;
SELECT id, data.a FROM t_pending ORDER BY id;

CREATE TABLE t_to_string (id UInt64, data JSON(a UInt32)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_to_string;
INSERT INTO t_to_string VALUES (1, '{"a" : 42}'), (2, '{"a" : 12345}');
ALTER TABLE t_to_string MODIFY COLUMN data String SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT id, length(data) FROM t_to_string ORDER BY id;
SELECT id, data FROM t_to_string ORDER BY id;

SELECT 'mutation';
CREATE TABLE t_mutation (id UInt64, data JSON(a UInt32)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_mutation VALUES (1, '{"a" : 42}'), (2, '{"a" : 12345}');
ALTER TABLE t_mutation MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
ALTER TABLE t_mutation DELETE WHERE empty(data.a) SETTINGS mutations_sync = 2;
SELECT id, data.a FROM t_mutation ORDER BY id;

SELECT 'array';
CREATE TABLE t_array (id UInt64, arr Array(JSON(a UInt32))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_array;
INSERT INTO t_array VALUES (1, ['{"a" : 42}', '{"a" : 7}']), (2, []);
ALTER TABLE t_array MODIFY COLUMN arr Array(JSON(a String)) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, arr.a.size FROM t_array ORDER BY id;
SELECT id, arr.a FROM t_array ORDER BY id;

SELECT 'single stream';
CREATE TABLE t_single_stream (id UInt64, data JSON(a UInt32)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, string_serialization_version = 'single_stream';
SYSTEM STOP MERGES t_single_stream;
INSERT INTO t_single_stream VALUES (1, '{"a" : 42}'), (2, '{"a" : 12345}');
ALTER TABLE t_single_stream MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, data.a.size, data.a.:String FROM t_single_stream ORDER BY id;

SELECT 'hint removed';
CREATE TABLE t_reverse (id UInt64, j JSON(n Nullable(UInt32))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_reverse;
INSERT INTO t_reverse VALUES (1, '{"n" : 5}'), (2, '{}');
ALTER TABLE t_reverse MODIFY COLUMN j JSON SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, j.n.null, isNull(j.n) FROM t_reverse ORDER BY id;
SYSTEM START MERGES t_reverse;
OPTIMIZE TABLE t_reverse FINAL;
SELECT id, j.n.null, isNull(j.n) FROM t_reverse ORDER BY id;

SELECT 'typed hint changed';
CREATE TABLE t_map_to_string (id UInt64, data JSON(a Map(String, UInt32))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_map_to_string;
INSERT INTO t_map_to_string VALUES (1, '{"a" : {"k" : 1}}');
ALTER TABLE t_map_to_string MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
SELECT data.a FROM t_map_to_string;
SYSTEM START MERGES t_map_to_string;
OPTIMIZE TABLE t_map_to_string FINAL;
SELECT data.a FROM t_map_to_string;

SELECT 'hint added';
CREATE TABLE t_add_hint (id UInt64, data JSON(b Array(UInt32))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_add_hint;
INSERT INTO t_add_hint VALUES (1, '{"a" : 42, "b" : [1], "c" : {"d" : 1}}'), (2, '{"a" : "str"}');
ALTER TABLE t_add_hint MODIFY COLUMN data JSON(a String, b Array(UInt32)) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, data.a, data.b.size0, data.^c, data.c.d FROM t_add_hint ORDER BY id;

CREATE TABLE t_typed_ancestor (id UInt64, data JSON) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_typed_ancestor;
INSERT INTO t_typed_ancestor VALUES (1, '{"b" : {"c" : 1}}');
ALTER TABLE t_typed_ancestor MODIFY COLUMN data JSON(b String) SETTINGS enable_json_lazy_type_hints = 1;
SELECT data.b.c FROM t_typed_ancestor;
SYSTEM START MERGES t_typed_ancestor;
OPTIMIZE TABLE t_typed_ancestor FINAL;
SELECT data.b.c FROM t_typed_ancestor;

CREATE TABLE t_skip (id UInt64, data JSON) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_skip;
INSERT INTO t_skip VALUES (1, '{"a" : 1, "b" : {"c" : 2}}');
ALTER TABLE t_skip MODIFY COLUMN data JSON(SKIP b) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT data.a, data.b.c FROM t_skip;

SELECT 'hints that can hold an object';
CREATE TABLE t_dynamic_hint (id UInt64, j JSON) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_dynamic_hint;
INSERT INTO t_dynamic_hint VALUES (1, '{"a" : {"k" : 1, "l" : 2}}'), (2, '{"a" : 42}'), (3, '{}');
ALTER TABLE t_dynamic_hint MODIFY COLUMN j JSON(a Dynamic) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, dynamicType(j.a), j.a, j.a.:Int64 FROM t_dynamic_hint ORDER BY id;
SYSTEM START MERGES t_dynamic_hint;
OPTIMIZE TABLE t_dynamic_hint FINAL;
SELECT id, dynamicType(j.a), j.a, j.a.:Int64 FROM t_dynamic_hint ORDER BY id;

CREATE TABLE t_variant_hint (id UInt64, j JSON) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_variant_hint;
INSERT INTO t_variant_hint VALUES (1, '{"a" : {"k" : 1, "l" : 2}}'), (2, '{"a" : "s"}'), (3, '{}');
ALTER TABLE t_variant_hint MODIFY COLUMN j JSON(a Variant(Map(String, UInt32), String)) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, variantType(j.a), j.a, j.a.String FROM t_variant_hint ORDER BY id;
SYSTEM START MERGES t_variant_hint;
OPTIMIZE TABLE t_variant_hint FINAL;
SELECT id, variantType(j.a), j.a, j.a.String FROM t_variant_hint ORDER BY id;

CREATE TABLE t_variant_in_part (id UInt64, j JSON(a Variant(Map(String, UInt32), String))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_variant_in_part;
INSERT INTO t_variant_in_part VALUES (1, '{"a" : {"k" : 1}}'), (2, '{"a" : "s"}');
ALTER TABLE t_variant_in_part MODIFY COLUMN j JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, j.a, length(j.a) FROM t_variant_in_part ORDER BY id;
SYSTEM START MERGES t_variant_in_part;
OPTIMIZE TABLE t_variant_in_part FINAL;
SELECT id, j.a, length(j.a) FROM t_variant_in_part ORDER BY id;

SELECT 'patch before alter';
CREATE TABLE t_patch_before (id UInt64, data JSON(a UInt32)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_patch_before;
INSERT INTO t_patch_before VALUES (1, '{"a" : 42}'), (2, '{"a" : 12345}');
UPDATE t_patch_before SET data = '{"a" : 777}' WHERE id = 1;
ALTER TABLE t_patch_before MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, length(data.a) FROM t_patch_before ORDER BY id;
SELECT id, data.a FROM t_patch_before ORDER BY id;

SELECT 'patch before alter, part merged without it';
CREATE TABLE t_patch_merged (id UInt64, data JSON(a UInt32)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;
INSERT INTO t_patch_merged VALUES (1, '{"a" : 42}'), (2, '{"a" : 12345}');
UPDATE t_patch_merged SET data = '{"a" : 777}' WHERE id = 1;
ALTER TABLE t_patch_merged MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
OPTIMIZE TABLE t_patch_merged FINAL;
SELECT id, data.a FROM t_patch_merged ORDER BY id;
SELECT id, length(data.a) FROM t_patch_merged ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, length(data.a) FROM t_patch_merged ORDER BY id;
SELECT id, data FROM t_patch_merged ORDER BY id;

SELECT 'patch after alter';
CREATE TABLE t_patch_after (id UInt64, data JSON(a UInt32)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_patch_after;
INSERT INTO t_patch_after VALUES (1, '{"a" : 42}'), (2, '{"a" : 12345}');
ALTER TABLE t_patch_after MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
UPDATE t_patch_after SET data = '{"a" : "xyz7"}' WHERE id = 1;
SELECT id, length(data.a) FROM t_patch_after ORDER BY id;
SELECT id, data.a FROM t_patch_after ORDER BY id;

SELECT 'patch after a pending mutation';
CREATE TABLE t_patch_after_mutation (id UInt64, j JSON(a UInt32), x UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_patch_after_mutation VALUES (1, '{"a" : 42}', 0), (2, '{"a" : 12345}', 0);
SYSTEM STOP MERGES t_patch_after_mutation;
ALTER TABLE t_patch_after_mutation MODIFY COLUMN j JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
ALTER TABLE t_patch_after_mutation UPDATE x = length(j.a) WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0, alter_update_mode = 'heavy';
UPDATE t_patch_after_mutation SET j = '{"a" : "abcdefgh"}' WHERE id = 1;
SELECT id, x, j, length(j.a) FROM t_patch_after_mutation ORDER BY id SETTINGS apply_mutations_on_fly = 1;
SELECT id, x FROM t_patch_after_mutation WHERE length(j.a) = 8 ORDER BY id SETTINGS apply_mutations_on_fly = 1;

CREATE TABLE t_old_patch_after_mutation (id UInt64, j JSON(a UInt32), x UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_old_patch_after_mutation VALUES (1, '{"a" : 42}', 0), (2, '{"a" : 12345}', 0);
SYSTEM STOP MERGES t_old_patch_after_mutation;
ALTER TABLE t_old_patch_after_mutation UPDATE x = length(toString(j.a)) WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0, alter_update_mode = 'heavy';
UPDATE t_old_patch_after_mutation SET j = '{"a" : 1234567}' WHERE id = 1;
ALTER TABLE t_old_patch_after_mutation MODIFY COLUMN j JSON(a String) SETTINGS enable_json_lazy_type_hints = 1, mutations_sync = 0, alter_sync = 0;
SELECT id, x, length(j.a) FROM t_old_patch_after_mutation ORDER BY id SETTINGS apply_mutations_on_fly = 1;

SELECT 'hint added, subcolumns from the cast path';
CREATE TABLE t_cast_path (id UInt64, j JSON) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_cast_path;
INSERT INTO t_cast_path VALUES (1, '{"a" : [1, 2], "n" : 5, "s" : "xyz"}'), (2, '{"a" : [3], "s" : ""}'), (3, '{}');
ALTER TABLE t_cast_path MODIFY COLUMN j JSON(a Array(UInt32), n Nullable(UInt32), s String) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, length(j.a), j.a.size0, j.n.null, length(j.s) FROM t_cast_path ORDER BY id;
SELECT id, length(j.a), j.a.size0, j.n.null, length(j.s) FROM t_cast_path ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_cast_path WHERE j.n IS NULL;
SELECT count() FROM t_cast_path WHERE j.n IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id FROM t_cast_path WHERE length(j.a) = 2 SETTINGS optimize_move_to_prewhere = 1;
SELECT id FROM t_cast_path WHERE length(j.a) = 2 SETTINGS optimize_move_to_prewhere = 1, optimize_functions_to_subcolumns = 0;
SELECT id, j.a.size0 FROM t_cast_path WHERE length(j.s) > 0 ORDER BY id SETTINGS optimize_move_to_prewhere = 1;
SELECT id, j.a.size0 FROM t_cast_path WHERE has(j.a, 3) ORDER BY id SETTINGS optimize_move_to_prewhere = 1;
SYSTEM START MERGES t_cast_path;
OPTIMIZE TABLE t_cast_path FINAL;
SELECT id, length(j.a), j.a.size0, j.n.null, length(j.s) FROM t_cast_path ORDER BY id;

CREATE TABLE t_cast_path_patch (id UInt64, j JSON) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_cast_path_patch;
INSERT INTO t_cast_path_patch VALUES (1, '{"a" : [1, 2]}'), (2, '{"a" : [3]}');
UPDATE t_cast_path_patch SET j = '{"a" : [4, 5, 6]}' WHERE id = 1;
ALTER TABLE t_cast_path_patch MODIFY COLUMN j JSON(a Array(UInt32)) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, length(j.a) FROM t_cast_path_patch ORDER BY id;

SELECT 'structured hint added in array';
CREATE TABLE t_array_map (id UInt64, arr Array(JSON)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_array_map;
INSERT INTO t_array_map VALUES (1, ['{"a" : {"k" : 1}}']), (2, ['{"a" : {"k" : 2, "l" : 3}}', '{}']);
ALTER TABLE t_array_map MODIFY COLUMN arr Array(JSON(a Map(String, UInt32))) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, arr.a FROM t_array_map ORDER BY id;
SYSTEM START MERGES t_array_map;
OPTIMIZE TABLE t_array_map FINAL;
SELECT id, arr.a FROM t_array_map ORDER BY id;

CREATE TABLE t_array_tuple (id UInt64, arr Array(JSON)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_array_tuple;
INSERT INTO t_array_tuple VALUES (1, ['{"a" : {"k" : 1}}']), (2, ['{"a" : {"k" : 2}}', '{}']);
ALTER TABLE t_array_tuple MODIFY COLUMN arr Array(JSON(a Tuple(k UInt32))) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, arr.a FROM t_array_tuple ORDER BY id;
SYSTEM START MERGES t_array_tuple;
OPTIMIZE TABLE t_array_tuple FINAL;
SELECT id, arr.a FROM t_array_tuple ORDER BY id;

SELECT 'nested member';
CREATE TABLE t_nested_wide (id UInt64, n Nested(j JSON(a UInt32))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_nested_wide;
INSERT INTO t_nested_wide VALUES (1, ['{"a" : 42}', '{"a" : 7}']), (2, []);
ALTER TABLE t_nested_wide MODIFY COLUMN `n.j` Array(JSON(a String)) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, n.j.a, n.j.a.size, arrayMap(x -> length(x), n.j.a) FROM t_nested_wide ORDER BY id;
SYSTEM START MERGES t_nested_wide;
OPTIMIZE TABLE t_nested_wide FINAL;
SELECT id, n.j.a, n.j.a.size, arrayMap(x -> length(x), n.j.a) FROM t_nested_wide ORDER BY id;

CREATE TABLE t_nested_compact (id UInt64, n Nested(j JSON(a UInt32))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '1G';
SYSTEM STOP MERGES t_nested_compact;
INSERT INTO t_nested_compact VALUES (1, ['{"a" : 42}', '{"a" : 7}']), (2, []);
ALTER TABLE t_nested_compact MODIFY COLUMN `n.j` Array(JSON(a String)) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, n.j.a, n.j.a.size, arrayMap(x -> length(x), n.j.a) FROM t_nested_compact ORDER BY id;
SYSTEM START MERGES t_nested_compact;
OPTIMIZE TABLE t_nested_compact FINAL;
SELECT id, n.j.a, n.j.a.size, arrayMap(x -> length(x), n.j.a) FROM t_nested_compact ORDER BY id;

CREATE TABLE t_nested_patch (id UInt64, n Nested(j JSON(a UInt32))) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_nested_patch;
INSERT INTO t_nested_patch VALUES (1, ['{"a" : 42}']), (2, ['{"a" : 12345}']);
UPDATE t_nested_patch SET `n.j` = ['{"a" : 777}'] WHERE id = 1;
ALTER TABLE t_nested_patch MODIFY COLUMN `n.j` Array(JSON(a String)) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, n.j.a, n.j.a.size FROM t_nested_patch ORDER BY id;

SELECT 'parent read by an earlier step';
CREATE TABLE t_parent_prewhere (id UInt64, data JSON(a UInt32)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_parent_prewhere;
INSERT INTO t_parent_prewhere VALUES (1, '{"a" : 42}'), (2, '{"a" : 12345}');
ALTER TABLE t_parent_prewhere MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, data.a.size FROM t_parent_prewhere WHERE toString(data) != '' ORDER BY id SETTINGS optimize_move_to_prewhere = 1;

CREATE TABLE t_nullable_prewhere (id UInt64, x UInt32) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_nullable_prewhere;
INSERT INTO t_nullable_prewhere VALUES (1, 5), (2, 6);
ALTER TABLE t_nullable_prewhere MODIFY COLUMN x Nullable(UInt32) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT id, x.null FROM t_nullable_prewhere WHERE x > 5 ORDER BY id SETTINGS optimize_move_to_prewhere = 1;

SELECT 'omitted, patch before alter';
CREATE TABLE t_omitted_patch_before (id UInt64, data JSON(a UInt32)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_omitted_patch_before;
INSERT INTO t_omitted_patch_before VALUES (1, '{}'), (2, '{}');
UPDATE t_omitted_patch_before SET data = '{"a" : 777}' WHERE id = 1;
ALTER TABLE t_omitted_patch_before MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, length(data.a), data.a.size FROM t_omitted_patch_before ORDER BY id;
SELECT id, length(data.a) FROM t_omitted_patch_before ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, data.a FROM t_omitted_patch_before ORDER BY id;
SELECT sum(length(data.a)) FROM t_omitted_patch_before;

SELECT 'omitted, patch after alter';
CREATE TABLE t_omitted_patch_after (id UInt64, data JSON(a UInt32)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_omitted_patch_after;
INSERT INTO t_omitted_patch_after VALUES (1, '{}'), (2, '{}');
ALTER TABLE t_omitted_patch_after MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
UPDATE t_omitted_patch_after SET data = '{"a" : "xyz7"}' WHERE id = 1;
SELECT id, length(data.a), data.a.size FROM t_omitted_patch_after ORDER BY id;
SELECT id, length(data.a) FROM t_omitted_patch_after ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, data.a FROM t_omitted_patch_after ORDER BY id;

SELECT 'column added after the part, patch in the old type';
CREATE TABLE t_absent_patch (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_absent_patch;
INSERT INTO t_absent_patch VALUES (1), (2);
ALTER TABLE t_absent_patch ADD COLUMN data JSON(a UInt32);
UPDATE t_absent_patch SET data = '{"a" : 777}' WHERE id = 1;
ALTER TABLE t_absent_patch MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, length(data.a), data.a.size FROM t_absent_patch ORDER BY id;
SELECT id, length(data.a) FROM t_absent_patch ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, data.a FROM t_absent_patch ORDER BY id;
SYSTEM START MERGES t_absent_patch;
OPTIMIZE TABLE t_absent_patch FINAL;
SELECT id, length(data.a), data.a FROM t_absent_patch ORDER BY id;

CREATE TABLE t_absent_patch_map (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_absent_patch_map;
INSERT INTO t_absent_patch_map VALUES (1), (2);
ALTER TABLE t_absent_patch_map ADD COLUMN data JSON(a Map(String, UInt32));
UPDATE t_absent_patch_map SET data = '{"a" : {"k" : 1}}' WHERE id = 1;
ALTER TABLE t_absent_patch_map MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, length(data.a), data.a.size FROM t_absent_patch_map ORDER BY id;
SELECT id, length(data.a) FROM t_absent_patch_map ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, data.a FROM t_absent_patch_map ORDER BY id;
SYSTEM START MERGES t_absent_patch_map;
OPTIMIZE TABLE t_absent_patch_map FINAL;
SELECT id, length(data.a), data.a FROM t_absent_patch_map ORDER BY id;

SELECT 'column added after the part with a default, patch in the old type';
CREATE TABLE t_absent_default_patch (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_absent_default_patch;
INSERT INTO t_absent_default_patch VALUES (1), (2);
ALTER TABLE t_absent_default_patch ADD COLUMN data JSON(a UInt32) DEFAULT '{"a" : 5}';
UPDATE t_absent_default_patch SET data = '{"a" : 777}' WHERE id = 1;
ALTER TABLE t_absent_default_patch MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, length(data.a), data.a.size FROM t_absent_default_patch ORDER BY id;
SELECT id, length(data.a) FROM t_absent_default_patch ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, data.a FROM t_absent_default_patch ORDER BY id;
SYSTEM START MERGES t_absent_default_patch;
OPTIMIZE TABLE t_absent_default_patch FINAL;
SELECT id, length(data.a), data.a FROM t_absent_default_patch ORDER BY id;

CREATE TABLE t_absent_default_patch_map (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_absent_default_patch_map;
INSERT INTO t_absent_default_patch_map VALUES (1), (2);
ALTER TABLE t_absent_default_patch_map ADD COLUMN data JSON(a Map(String, UInt32)) DEFAULT '{"a" : {"x" : 22}}';
UPDATE t_absent_default_patch_map SET data = '{"a" : {"k" : 1}}' WHERE id = 1;
ALTER TABLE t_absent_default_patch_map MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, length(data.a), data.a.size FROM t_absent_default_patch_map ORDER BY id;
SELECT id, length(data.a) FROM t_absent_default_patch_map ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, data.a FROM t_absent_default_patch_map ORDER BY id;
SYSTEM START MERGES t_absent_default_patch_map;
OPTIMIZE TABLE t_absent_default_patch_map FINAL;
SELECT id, length(data.a), data.a FROM t_absent_default_patch_map ORDER BY id;

SELECT 'text index fallback';
CREATE TABLE t_text_index_typed (id UInt64, doc JSON(a String)) ENGINE = MergeTree PARTITION BY id < 1000 ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_text_index_typed SELECT number, if(number < 100, '{"a" : "actor003"}', '{"a" : "actor004"}') FROM numbers(200);
ALTER TABLE t_text_index_typed MODIFY COLUMN doc JSON(a FixedString(8)) SETTINGS enable_json_lazy_type_hints = 1;
ALTER TABLE t_text_index_typed ADD INDEX idx doc.a TYPE text(tokenizer = array) GRANULARITY 100000000;
INSERT INTO t_text_index_typed VALUES (1000, '{"a" : "actor003"}');
ALTER TABLE t_text_index_typed MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
SELECT count(), sum(id) FROM t_text_index_typed WHERE doc.a LIKE '%actor003%'
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1,
    use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1, text_index_like_max_postings_to_read = 0;
SELECT count(), sum(id) FROM t_text_index_typed WHERE doc.a LIKE '%actor003%' SETTINGS use_skip_indexes = 0;

CREATE TABLE t_text_index_map (id UInt64, doc JSON(a Map(String, String))) ENGINE = MergeTree PARTITION BY id < 1000 ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_text_index_map SELECT number, if(number < 100, '{"a" : {"k" : "actor003"}}', '{"a" : {"k" : "actor004"}}') FROM numbers(200);
ALTER TABLE t_text_index_map MODIFY COLUMN doc JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
ALTER TABLE t_text_index_map ADD INDEX idx doc.a TYPE text(tokenizer = array) GRANULARITY 100000000;
INSERT INTO t_text_index_map VALUES (1000, '{"a" : "{\\"k\\":\\"actor003\\"}"}');
SELECT count(), sum(id) FROM t_text_index_map WHERE doc.a LIKE '%"k":"actor003"%'
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1,
    use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1, text_index_like_max_postings_to_read = 0;
ALTER TABLE t_text_index_map MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
SELECT count(), sum(id) FROM t_text_index_map WHERE doc.a LIKE '%"k":"actor003"%'
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, query_plan_direct_read_from_text_index = 1, query_plan_text_index_add_hint = 1,
    use_text_index_like_evaluation_by_dictionary_scan = 1, text_index_like_min_pattern_length = 1, text_index_like_max_postings_to_read = 0;
SELECT count(), sum(id) FROM t_text_index_map WHERE doc.a LIKE '%"k":"actor003"%' SETTINGS use_skip_indexes = 0;

SELECT 'default of another column over a subcolumn';
CREATE TABLE t_default_dep (id UInt64, data JSON(a Map(String, UInt32))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_default_dep;
INSERT INTO t_default_dep VALUES (1, '{"a" : {"k" : 1}}'), (2, '{}');
ALTER TABLE t_default_dep MODIFY COLUMN data JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
ALTER TABLE t_default_dep ADD COLUMN x String DEFAULT data.a;
SELECT id, x FROM t_default_dep ORDER BY id;
SYSTEM START MERGES t_default_dep;
OPTIMIZE TABLE t_default_dep FINAL;
SELECT id, x FROM t_default_dep ORDER BY id;

SELECT 'subcolumns from the cast path read only the path';
CREATE TABLE t_read_cast_path_added (id UInt64, j JSON) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_read_cast_path_added;
INSERT INTO t_read_cast_path_added SELECT number, concat('{"a" : [', toString(number), '], "big" : "', repeat('x', 100000), '"}') FROM numbers(3);
ALTER TABLE t_read_cast_path_added MODIFY COLUMN j JSON(a Array(UInt32)) SETTINGS enable_json_lazy_type_hints = 1;
SELECT sum(length(j.a)), sum(j.a.size0) FROM t_read_cast_path_added SETTINGS log_comment = '05508_read_cast_path_added';

CREATE TABLE t_read_cast_path_changed (id UInt64, j JSON(a UInt32)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_read_cast_path_changed;
INSERT INTO t_read_cast_path_changed SELECT number, concat('{"a" : ', toString(number * 100), ', "big" : "', repeat('x', 100000), '"}') FROM numbers(3);
ALTER TABLE t_read_cast_path_changed MODIFY COLUMN j JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
SELECT sum(length(j.a)), sum(j.a.size) FROM t_read_cast_path_changed SETTINGS log_comment = '05508_read_cast_path_changed';

CREATE TABLE t_read_cast_path_array (id UInt64, arr Array(JSON(a UInt32))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_read_cast_path_array;
INSERT INTO t_read_cast_path_array SELECT number, [concat('{"a" : ', toString(number * 100), ', "big" : "', repeat('x', 100000), '"}')] FROM numbers(3);
ALTER TABLE t_read_cast_path_array MODIFY COLUMN arr Array(JSON(a String)) SETTINGS enable_json_lazy_type_hints = 1;
SELECT sum(arraySum(arr.a.size)) FROM t_read_cast_path_array SETTINGS log_comment = '05508_read_cast_path_array';

CREATE TABLE t_read_cast_path_tuple (id UInt64, tup Tuple(t JSON(a UInt32))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_read_cast_path_tuple;
INSERT INTO t_read_cast_path_tuple SELECT number, CAST(tuple(concat('{"a" : ', toString(number * 100), ', "big" : "', repeat('x', 100000), '"}')), 'Tuple(t JSON(a UInt32))') FROM numbers(3);
ALTER TABLE t_read_cast_path_tuple MODIFY COLUMN tup Tuple(t JSON(a String)) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT sum(length(tup.t.a)), sum(tup.t.a.size) FROM t_read_cast_path_tuple SETTINGS log_comment = '05508_read_cast_path_tuple';

CREATE TABLE t_read_cast_path_map (id UInt64, m Map(String, JSON(a UInt32))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_read_cast_path_map;
INSERT INTO t_read_cast_path_map SELECT number, map('k', concat('{"a" : ', toString(number * 100), ', "big" : "', repeat('x', 100000), '"}')) FROM numbers(3);
ALTER TABLE t_read_cast_path_map MODIFY COLUMN m Map(String, JSON(a String)) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT sum(arraySum(m.values.a.size)) FROM t_read_cast_path_map SETTINGS log_comment = '05508_read_cast_path_map';

SELECT 'a scalar member next to a JSON member is read and cast as stored';
CREATE TABLE t_scalar_sibling (id UInt64, tup Tuple(a UInt64, b String, j JSON)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_scalar_sibling;
INSERT INTO t_scalar_sibling SELECT number, CAST(tuple(number * 7, 'x', concat('{"big" : "', repeat('x', 100000), '"}')), 'Tuple(a UInt64, b String, j JSON)') FROM numbers(3);
ALTER TABLE t_scalar_sibling MODIFY COLUMN tup Tuple(a String, b UInt64, j JSON) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT id, tup.a FROM t_scalar_sibling ORDER BY id SETTINGS log_comment = '05508_read_scalar_sibling';

CREATE TABLE t_scalar_sibling_map (id UInt64, m Map(String, Tuple(a UInt64, b String, j JSON))) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_scalar_sibling_map;
INSERT INTO t_scalar_sibling_map SELECT number, map('k', CAST(tuple(number * 7, 'x', concat('{"big" : "', repeat('x', 100000), '"}')), 'Tuple(a UInt64, b String, j JSON)')) FROM numbers(3);
ALTER TABLE t_scalar_sibling_map MODIFY COLUMN m Map(String, Tuple(a String, b UInt64, j JSON)) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT id, m.values.a FROM t_scalar_sibling_map ORDER BY id SETTINGS log_comment = '05508_read_scalar_sibling_map';

CREATE TABLE t_scalar_sibling_nullable_wide (id UInt64, tup Tuple(a String, b String, j JSON)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES t_scalar_sibling_nullable_wide;
INSERT INTO t_scalar_sibling_nullable_wide VALUES (1, ('42', 'bad', '{}'));
ALTER TABLE t_scalar_sibling_nullable_wide MODIFY COLUMN tup Tuple(a Nullable(String), b UInt64, j JSON) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT length(tup.a), tup.a.size FROM t_scalar_sibling_nullable_wide;

CREATE TABLE t_scalar_sibling_nullable_compact (id UInt64, tup Tuple(a String, b String, j JSON)) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '1G';
SYSTEM STOP MERGES t_scalar_sibling_nullable_compact;
INSERT INTO t_scalar_sibling_nullable_compact VALUES (1, ('42', 'bad', '{}'));
ALTER TABLE t_scalar_sibling_nullable_compact MODIFY COLUMN tup Tuple(a Nullable(String), b UInt64, j JSON) SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT length(tup.a), tup.a.size FROM t_scalar_sibling_nullable_compact;

SELECT 'the source of a subcolumn a pending mutation reads stays readable';
CREATE TABLE t_mutation_source_wide (id UInt64, j JSON(a UInt32), x UInt64) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_mutation_source_wide VALUES (1, '{"a" : 42}', 0), (2, '{"a" : 12345}', 0);
SYSTEM STOP MERGES t_mutation_source_wide;
ALTER TABLE t_mutation_source_wide MODIFY COLUMN j JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
ALTER TABLE t_mutation_source_wide UPDATE x = j.a.size WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0, alter_update_mode = 'heavy';
SELECT id, x, j.a FROM t_mutation_source_wide ORDER BY id SETTINGS apply_mutations_on_fly = 1;
SELECT id, j.a FROM t_mutation_source_wide WHERE x > 2 ORDER BY id SETTINGS apply_mutations_on_fly = 1;

CREATE TABLE t_mutation_source_compact (id UInt64, j JSON(a UInt32), x UInt64) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '1G';
INSERT INTO t_mutation_source_compact VALUES (1, '{"a" : 42}', 0), (2, '{"a" : 12345}', 0);
SYSTEM STOP MERGES t_mutation_source_compact;
ALTER TABLE t_mutation_source_compact MODIFY COLUMN j JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
ALTER TABLE t_mutation_source_compact UPDATE x = j.a.size WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0, alter_update_mode = 'heavy';
SELECT id, x, j.a FROM t_mutation_source_compact ORDER BY id SETTINGS apply_mutations_on_fly = 1;

SELECT 'a source read again after a pending mutation gets the patches written before it';
CREATE TABLE t_mutation_source_patch_wide (id UInt64, j JSON(a UInt32), x UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_mutation_source_patch_wide VALUES (1, '{"a" : 42}', 0), (2, '{"a" : 12345}', 0);
SYSTEM STOP MERGES t_mutation_source_patch_wide;
ALTER TABLE t_mutation_source_patch_wide MODIFY COLUMN j JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
UPDATE t_mutation_source_patch_wide SET j = '{"a" : "abcdefgh"}' WHERE id = 1;
ALTER TABLE t_mutation_source_patch_wide UPDATE x = j.a.size WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0, alter_update_mode = 'heavy';
SELECT id, x, j.a FROM t_mutation_source_patch_wide ORDER BY id SETTINGS apply_mutations_on_fly = 1;
SELECT id, x, j.a FROM t_mutation_source_patch_wide WHERE j.a = 'abcdefgh' SETTINGS apply_mutations_on_fly = 1;
SELECT id, x, j.a FROM t_mutation_source_patch_wide WHERE j.a = 'abcdefgh' SETTINGS apply_mutations_on_fly = 1, optimize_move_to_prewhere = 0;

CREATE TABLE t_mutation_source_patch_compact (id UInt64, j JSON(a UInt32), x UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '1G', enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_mutation_source_patch_compact VALUES (1, '{"a" : 42}', 0), (2, '{"a" : 12345}', 0);
SYSTEM STOP MERGES t_mutation_source_patch_compact;
ALTER TABLE t_mutation_source_patch_compact MODIFY COLUMN j JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
UPDATE t_mutation_source_patch_compact SET j = '{"a" : "abcdefgh"}' WHERE id = 1;
ALTER TABLE t_mutation_source_patch_compact UPDATE x = j.a.size WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0, alter_update_mode = 'heavy';
SELECT id, x, j.a FROM t_mutation_source_patch_compact ORDER BY id SETTINGS apply_mutations_on_fly = 1;
SELECT id, x, j.a FROM t_mutation_source_patch_compact WHERE j.a = 'abcdefgh' SETTINGS apply_mutations_on_fly = 1;

SELECT 'a subcolumn passed between pending mutations gets the patches written between them';
CREATE TABLE t_chained_mutations (id UInt64, j JSON(a UInt32), x UInt64, y UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_chained_mutations VALUES (1, '{"a" : 42}', 0, 0), (2, '{"a" : 12345}', 0, 0);
SYSTEM STOP MERGES t_chained_mutations;
ALTER TABLE t_chained_mutations MODIFY COLUMN j JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
ALTER TABLE t_chained_mutations UPDATE x = j.a.size WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0, alter_update_mode = 'heavy';
UPDATE t_chained_mutations SET j = '{"a" : "abc"}' WHERE id = 2;
ALTER TABLE t_chained_mutations UPDATE y = j.a.size WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0, alter_update_mode = 'heavy';
SELECT id, x, y, j.a FROM t_chained_mutations ORDER BY id SETTINGS apply_mutations_on_fly = 1;

SELECT 'a subcolumn passed between pending mutations gets the patches of an older type written between them';
CREATE TABLE t_chained_mutations_old_patch_wide (id UInt64, j JSON(a Array(UInt32)), x UInt64, y UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_chained_mutations_old_patch_wide VALUES (1, '{"a" : [1, 2]}', 0, 0), (2, '{"a" : [3, 4]}', 0, 0);
SYSTEM STOP MERGES t_chained_mutations_old_patch_wide;
ALTER TABLE t_chained_mutations_old_patch_wide UPDATE x = j.a.size0 WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0, alter_update_mode = 'heavy';
UPDATE t_chained_mutations_old_patch_wide SET j = '{"a" : [7, 8, 9]}' WHERE id = 2;
ALTER TABLE t_chained_mutations_old_patch_wide UPDATE y = j.a.size0 WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0, alter_update_mode = 'heavy';
ALTER TABLE t_chained_mutations_old_patch_wide MODIFY COLUMN j JSON(a Array(String)) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, x, y, j.a FROM t_chained_mutations_old_patch_wide ORDER BY id SETTINGS apply_mutations_on_fly = 1;
SELECT id, x, y FROM t_chained_mutations_old_patch_wide WHERE y = 3 SETTINGS apply_mutations_on_fly = 1;
CREATE TABLE t_chained_mutations_old_patch_compact (id UInt64, j JSON(a Array(UInt32)), x UInt64, y UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '1G', enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_chained_mutations_old_patch_compact VALUES (1, '{"a" : [1, 2]}', 0, 0), (2, '{"a" : [3, 4]}', 0, 0);
SYSTEM STOP MERGES t_chained_mutations_old_patch_compact;
ALTER TABLE t_chained_mutations_old_patch_compact UPDATE x = j.a.size0 WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0, alter_update_mode = 'heavy';
UPDATE t_chained_mutations_old_patch_compact SET j = '{"a" : [7, 8, 9]}' WHERE id = 2;
ALTER TABLE t_chained_mutations_old_patch_compact UPDATE y = j.a.size0 WHERE 1 SETTINGS mutations_sync = 0, alter_sync = 0, alter_update_mode = 'heavy';
ALTER TABLE t_chained_mutations_old_patch_compact MODIFY COLUMN j JSON(a Array(String)) SETTINGS enable_json_lazy_type_hints = 1;
SELECT id, x, y, j.a FROM t_chained_mutations_old_patch_compact ORDER BY id SETTINGS apply_mutations_on_fly = 1;

SELECT 'a subcolumn of a path is not read from another column named as the path';
CREATE TABLE t_shadowed_path_wide (id UInt64, j JSON(a UInt32), `j.a` UInt64) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_shadowed_path_wide VALUES (1, '{"a" : 42}', 999999);
SYSTEM STOP MERGES t_shadowed_path_wide;
ALTER TABLE t_shadowed_path_wide MODIFY COLUMN j JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
SELECT j.a.size, `j.a` FROM t_shadowed_path_wide;
CREATE TABLE t_shadowed_path_compact (id UInt64, j JSON(a UInt32), `j.a` UInt64) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = '1G';
INSERT INTO t_shadowed_path_compact VALUES (1, '{"a" : 42}', 999999);
SYSTEM STOP MERGES t_shadowed_path_compact;
ALTER TABLE t_shadowed_path_compact MODIFY COLUMN j JSON(a String) SETTINGS enable_json_lazy_type_hints = 1;
SELECT j.a.size, `j.a` FROM t_shadowed_path_compact;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, read_bytes < 50000 FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment LIKE '05508_read_%' AND type = 'QueryFinish'
ORDER BY log_comment;

DROP TABLE t_issue;
DROP TABLE t_wide;
DROP TABLE t_compact;
DROP TABLE t_omitted;
DROP TABLE t_pending;
DROP TABLE t_to_string;
DROP TABLE t_mutation;
DROP TABLE t_array;
DROP TABLE t_single_stream;
DROP TABLE t_reverse;
DROP TABLE t_map_to_string;
DROP TABLE t_add_hint;
DROP TABLE t_typed_ancestor;
DROP TABLE t_skip;
DROP TABLE t_dynamic_hint;
DROP TABLE t_variant_hint;
DROP TABLE t_variant_in_part;
DROP TABLE t_patch_before;
DROP TABLE t_patch_merged;
DROP TABLE t_patch_after;
DROP TABLE t_patch_after_mutation;
DROP TABLE t_old_patch_after_mutation;
DROP TABLE t_cast_path;
DROP TABLE t_cast_path_patch;
DROP TABLE t_array_map;
DROP TABLE t_array_tuple;
DROP TABLE t_nested_wide;
DROP TABLE t_nested_compact;
DROP TABLE t_nested_patch;
DROP TABLE t_parent_prewhere;
DROP TABLE t_nullable_prewhere;
DROP TABLE t_omitted_patch_before;
DROP TABLE t_omitted_patch_after;
DROP TABLE t_absent_patch;
DROP TABLE t_absent_patch_map;
DROP TABLE t_absent_default_patch;
DROP TABLE t_absent_default_patch_map;
DROP TABLE t_text_index_typed;
DROP TABLE t_text_index_map;
DROP TABLE t_read_cast_path_added;
DROP TABLE t_read_cast_path_changed;
DROP TABLE t_read_cast_path_array;
DROP TABLE t_read_cast_path_tuple;
DROP TABLE t_scalar_sibling;
DROP TABLE t_read_cast_path_map;
DROP TABLE t_scalar_sibling_map;
DROP TABLE t_default_dep;
DROP TABLE t_mutation_source_wide;
DROP TABLE t_mutation_source_compact;
DROP TABLE t_mutation_source_patch_wide;
DROP TABLE t_mutation_source_patch_compact;
DROP TABLE t_chained_mutations;
DROP TABLE t_chained_mutations_old_patch_wide;
DROP TABLE t_chained_mutations_old_patch_compact;
DROP TABLE t_shadowed_path_wide;
DROP TABLE t_shadowed_path_compact;
DROP TABLE t_scalar_sibling_nullable_wide;
DROP TABLE t_scalar_sibling_nullable_compact;
