-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas

-- The expressions of a partial index are calculated only for the rows its condition selects. `intDiv(10, p)` and
-- `intDiv(id, p)` fail for the rows with `p = 0`, which the indexes do not cover.

SET allow_experimental_partial_skip_indexes = 1;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_div;
DROP TABLE IF EXISTS t_mat;
DROP TABLE IF EXISTS t_compact;

-- `i` reads one column, so a vertical merge calculates it with that column; `j` reads two.
CREATE TABLE t_div
(
    id UInt64,
    p UInt8,
    INDEX i intDiv(10, p) TYPE minmax GRANULARITY 1 WHERE p != 0,
    INDEX j intDiv(id, p) TYPE set(10) GRANULARITY 1 WHERE p != 0
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

SELECT 'insert';
-- Granules of two rows; the last one has no row the indexes cover.
INSERT INTO t_div VALUES (1, 0), (2, 5), (3, 0), (4, 2), (5, 0), (6, 0);
SELECT count() FROM t_div WHERE p != 0 AND intDiv(10, p) = 5 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_div WHERE p != 0 AND intDiv(10, p) = 5
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i', max_rows_to_read = 2;
SELECT count() FROM t_div WHERE p != 0 AND intDiv(10, p) = 5
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'i';
SELECT count() FROM t_div WHERE p != 0 AND intDiv(id, p) = 2 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_div WHERE p != 0 AND intDiv(id, p) = 2
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'j', ignore_data_skipping_indices = 'i';
SELECT count() FROM t_div WHERE p != 0 AND intDiv(id, p) = 2
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'j', ignore_data_skipping_indices = 'i';

SELECT 'merge';
INSERT INTO t_div VALUES (7, 0), (8, 1), (9, 0), (10, 0);
OPTIMIZE TABLE t_div FINAL;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_div' AND active;
SELECT count() FROM t_div WHERE p != 0 AND intDiv(10, p) = 10 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_div WHERE p != 0 AND intDiv(10, p) = 10
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i', max_rows_to_read = 2;
SELECT count() FROM t_div WHERE p != 0 AND intDiv(10, p) = 10
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'i';
SELECT count() FROM t_div WHERE p != 0 AND intDiv(id, p) = 8 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_div WHERE p != 0 AND intDiv(id, p) = 8
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'j', ignore_data_skipping_indices = 'i';
SELECT count() FROM t_div WHERE p != 0 AND intDiv(id, p) = 8
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'j', ignore_data_skipping_indices = 'i';

SELECT 'mutation';
-- One row leaves the indexes and one enters them.
ALTER TABLE t_div UPDATE p = 0 WHERE id = 4 SETTINGS mutations_sync = 2;
ALTER TABLE t_div UPDATE p = 10 WHERE id = 9 SETTINGS mutations_sync = 2;
SELECT count() FROM t_div WHERE p != 0 AND intDiv(10, p) = 5 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_div WHERE p != 0 AND intDiv(10, p) = 5
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i';
SELECT count() FROM t_div WHERE p != 0 AND intDiv(10, p) = 5
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'i';
SELECT count() FROM t_div WHERE p != 0 AND intDiv(10, p) = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_div WHERE p != 0 AND intDiv(10, p) = 1
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i', max_rows_to_read = 2;
SELECT count() FROM t_div WHERE p != 0 AND intDiv(10, p) = 1
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'i';
SELECT count() FROM t_div WHERE p != 0 AND intDiv(id, p) = 0 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_div WHERE p != 0 AND intDiv(id, p) = 0
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'j', ignore_data_skipping_indices = 'i';
SELECT count() FROM t_div WHERE p != 0 AND intDiv(id, p) = 0
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'j', ignore_data_skipping_indices = 'i';

SELECT 'MATERIALIZE INDEX';
CREATE TABLE t_mat (id UInt64, p UInt8) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2, min_bytes_for_wide_part = 0;
INSERT INTO t_mat VALUES (1, 0), (2, 5), (3, 0), (4, 2), (5, 0), (6, 0);
ALTER TABLE t_mat ADD INDEX i intDiv(10, p) TYPE minmax GRANULARITY 1 WHERE p != 0;
ALTER TABLE t_mat MATERIALIZE INDEX i SETTINGS mutations_sync = 2;
SELECT count() FROM t_mat WHERE p != 0 AND intDiv(10, p) = 5 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_mat WHERE p != 0 AND intDiv(10, p) = 5
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i', max_rows_to_read = 2;
SELECT count() FROM t_mat WHERE p != 0 AND intDiv(10, p) = 5
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'i';

SELECT 'Compact part';
CREATE TABLE t_compact (id UInt64, p UInt8, INDEX i intDiv(10, p) TYPE minmax GRANULARITY 1 WHERE p != 0)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 2, min_bytes_for_wide_part = '100G';
INSERT INTO t_compact VALUES (1, 0), (2, 5), (3, 0), (4, 2), (5, 0), (6, 0);
SELECT count() FROM t_compact WHERE p != 0 AND intDiv(10, p) = 5 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_compact WHERE p != 0 AND intDiv(10, p) = 5
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i', max_rows_to_read = 2;
SELECT count() FROM t_compact WHERE p != 0 AND intDiv(10, p) = 5
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'i';

DROP TABLE t_div;
DROP TABLE t_mat;
DROP TABLE t_compact;
