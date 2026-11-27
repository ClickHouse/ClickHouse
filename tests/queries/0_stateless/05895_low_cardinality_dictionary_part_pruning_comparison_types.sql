-- Pruning by the dictionaries of `LowCardinality` columns evaluates the filter on the values of the parts
-- that the primary key skips, so a comparison is used only if it cannot throw for the types of its arguments.

SET allow_suspicious_low_cardinality_types = 1;
SET optimize_use_implicit_projections = 0;
SET min_marks_per_part_to_load_statistics = 0;
SET use_statistics_for_part_pruning = 0;
SET optimize_move_to_prewhere = 0;
SET decimal_check_overflow = 1;

DROP TABLE IF EXISTS t_lc_comparison;
CREATE TABLE t_lc_comparison (id UInt64, v LowCardinality(UInt64), d LowCardinality(Date)) ENGINE = MergeTree ORDER BY id
SETTINGS storage_policy = 'default', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, write_low_cardinality_complete_dictionaries_marker = 1, auto_statistics_types = '';
SYSTEM STOP MERGES t_lc_comparison;
INSERT INTO t_lc_comparison VALUES (0, 0, '2020-01-01');
INSERT INTO t_lc_comparison VALUES (100, 18446744073709551615, '2021-01-01');

-- `UInt64` is converted to compare it with `Decimal`, and the maximum value of the skipped part does not fit.
SELECT 'decimal', count() FROM t_lc_comparison WHERE id = 0 AND v = materialize(toDecimal64(0, 0)) SETTINGS use_low_cardinality_dictionaries_for_part_pruning = 0;
SELECT 'decimal', count() FROM t_lc_comparison WHERE id = 0 AND v = materialize(toDecimal64(0, 0)) SETTINGS use_low_cardinality_dictionaries_for_part_pruning = 1;
SELECT 'decimal constant', count() FROM t_lc_comparison WHERE id = 0 AND v = toDecimal64(0, 0) SETTINGS use_low_cardinality_dictionaries_for_part_pruning = 1;
SELECT 'decimal less', count() FROM t_lc_comparison WHERE id = 0 AND v < toDecimal64(5, 0) SETTINGS use_low_cardinality_dictionaries_for_part_pruning = 1;
SELECT 'decimal not', count() FROM t_lc_comparison WHERE id = 0 AND NOT (v != toDecimal64(0, 0)) SETTINGS use_low_cardinality_dictionaries_for_part_pruning = 1;
SELECT 'decimal parts', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_comparison WHERE v = toDecimal64(0, 0)) SETTINGS use_low_cardinality_dictionaries_for_part_pruning = 1;

-- The comparisons that cannot throw still skip the parts.
SELECT 'number', count() FROM t_lc_comparison WHERE v = 0 SETTINGS use_low_cardinality_dictionaries_for_part_pruning = 1;
SELECT 'number parts', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_comparison WHERE v = 0) SETTINGS use_low_cardinality_dictionaries_for_part_pruning = 1;
SELECT 'constant date', count() FROM t_lc_comparison WHERE d = '2021-01-01' SETTINGS use_low_cardinality_dictionaries_for_part_pruning = 1;
SELECT 'constant date parts', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_comparison WHERE d = '2021-01-01') SETTINGS use_low_cardinality_dictionaries_for_part_pruning = 1;
SELECT 'date type parts', sum(parts) FROM (EXPLAIN ESTIMATE SELECT id FROM t_lc_comparison WHERE d < toDate('2020-06-01')) SETTINGS use_low_cardinality_dictionaries_for_part_pruning = 1;

DROP TABLE t_lc_comparison;
