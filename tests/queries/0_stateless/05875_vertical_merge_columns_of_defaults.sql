-- Vertical merges of columns that hold only defaults in every source part.

SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_defaults_wide;
DROP TABLE IF EXISTS t_defaults_replacing;
DROP TABLE IF EXISTS t_defaults_text;

SELECT 'Wide parts, lightweight delete, then a lightweight update';

CREATE TABLE t_defaults_wide
(
    k UInt64,
    v UInt64,
    e_u UInt64,
    e_s String,
    e_f Float64,
    e_n Nullable(UInt32),
    e_i UInt32,
    e_p UInt64,
    e_x UInt64,
    INDEX idx_i e_i TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY k
SETTINGS
    min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    compute_exact_num_defaults_for_sparse_columns = 1,
    serialization_info_version = 'with_types',
    nullable_serialization_version = 'allow_sparse',
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    apply_patches_on_merge = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_defaults_wide (k, v, e_x) SELECT number * 3, number, if(number = 5, 1, 0) FROM numbers(3000);
INSERT INTO t_defaults_wide (k, v) SELECT number * 3 + 1, number FROM numbers(3000);
INSERT INTO t_defaults_wide (k, v) SELECT number * 3 + 2, number FROM numbers(3000);

DELETE FROM t_defaults_wide WHERE k % 100 = 0;
OPTIMIZE TABLE t_defaults_wide FINAL;

UPDATE t_defaults_wide SET e_p = 7 WHERE k = 4;
OPTIMIZE TABLE t_defaults_wide FINAL;

SELECT count(), sum(v), countIf(e_u != 0), countIf(e_s != ''), countIf(e_f != 0), countIf(e_n IS NOT NULL), countIf(e_i != 0), countIf(e_p != 0), sum(e_p), countIf(e_x != 0), sum(e_x)
FROM t_defaults_wide;
SELECT e_p FROM t_defaults_wide WHERE k = 4 SETTINGS apply_patch_parts = 0;
SELECT count() FROM t_defaults_wide WHERE e_i = 0 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'idx_i';
SELECT count() FROM t_defaults_wide WHERE e_i = 1 SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'idx_i';
SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_defaults_wide' AND active AND NOT startsWith(name, 'patch') AND NOT startsWith(column, '_')
ORDER BY column;

SELECT 'Compact parts with duplicate keys merged into a Wide part';

CREATE TABLE t_defaults_replacing
(
    k UInt64,
    ver UInt64,
    v UInt64,
    e_u UInt64,
    e_s String,
    e_f Float64
)
ENGINE = ReplacingMergeTree(ver) ORDER BY k
SETTINGS
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 2500,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    compute_exact_num_defaults_for_sparse_columns = 1,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    allow_vertical_merges_from_compact_to_wide_parts = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_defaults_replacing (k, ver, v) SELECT number, 1, number FROM numbers(1000);
INSERT INTO t_defaults_replacing (k, ver, v) SELECT number + 500, 2, number FROM numbers(1000);
INSERT INTO t_defaults_replacing (k, ver, v) SELECT number + 1000, 3, number FROM numbers(1000);

SELECT part_type, count() FROM system.parts WHERE database = currentDatabase() AND table = 't_defaults_replacing' AND active GROUP BY part_type;
OPTIMIZE TABLE t_defaults_replacing FINAL;
SELECT part_type, count() FROM system.parts WHERE database = currentDatabase() AND table = 't_defaults_replacing' AND active GROUP BY part_type;

SELECT count(), sum(v), sum(ver), countIf(e_u != 0), countIf(e_s != ''), countIf(e_f != 0) FROM t_defaults_replacing;

SELECT 'A text index on one of two columns that hold only defaults';

CREATE TABLE t_defaults_text
(
    k UInt64,
    v UInt64,
    e_t String,
    e_s String,
    INDEX idx_t e_t TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY k
SETTINGS
    min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 0.9,
    compute_exact_num_defaults_for_sparse_columns = 1,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_defaults_text (k, v) SELECT number * 2, number FROM numbers(3000);
INSERT INTO t_defaults_text (k, v) SELECT number * 2 + 1, number FROM numbers(3000);
OPTIMIZE TABLE t_defaults_text FINAL;

SELECT count(), sum(v), countIf(e_t != ''), countIf(e_s != '') FROM t_defaults_text;
SELECT count() FROM t_defaults_text WHERE hasToken(e_t, 'abc');
SELECT count() FROM t_defaults_text WHERE e_t = '' AND e_s = '';

DROP TABLE t_defaults_wide;
DROP TABLE t_defaults_replacing;
DROP TABLE t_defaults_text;
