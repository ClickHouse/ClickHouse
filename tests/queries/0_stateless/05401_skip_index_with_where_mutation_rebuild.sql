-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas

-- A partial index whose marker is for another definition (here after a rename of its condition column) is not read, and
-- a mutation that changes data drops it: carried over, it would describe old data once the column is renamed back.

SET allow_experimental_partial_skip_indexes = 1;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_packed;
DROP TABLE IF EXISTS t_files;
DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_expr;
DROP TABLE IF EXISTS t_delete;
DROP TABLE IF EXISTS t_other;
DROP TABLE IF EXISTS t_rename;

SELECT 'Wide part, index in the packed archive';
CREATE TABLE t_packed (id UInt64, p UInt8, INDEX i id TYPE minmax WHERE p = 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_packed VALUES (1, 0);
ALTER TABLE t_packed RENAME COLUMN p TO pp SETTINGS mutations_sync = 2;
ALTER TABLE t_packed UPDATE pp = 1 WHERE id = 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_packed RENAME COLUMN pp TO p SETTINGS mutations_sync = 2;
SELECT count() FROM t_packed WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_packed WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_packed WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
ALTER TABLE t_packed MATERIALIZE INDEX i SETTINGS mutations_sync = 2;
SELECT count() FROM t_packed WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i';
SELECT count() FROM t_packed WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1, force_data_skipping_indices = 'i';

SELECT 'Wide part, index in its own files';
CREATE TABLE t_files (id UInt64, p UInt8, INDEX i id TYPE minmax WHERE p = 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0;
INSERT INTO t_files VALUES (1, 0);
ALTER TABLE t_files RENAME COLUMN p TO pp SETTINGS mutations_sync = 2;
ALTER TABLE t_files UPDATE pp = 1 WHERE id = 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_files RENAME COLUMN pp TO p SETTINGS mutations_sync = 2;
SELECT count() FROM t_files WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_files WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_files WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'Compact part';
CREATE TABLE t_compact (id UInt64, p UInt8, INDEX i id TYPE minmax WHERE p = 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = '100G';
INSERT INTO t_compact VALUES (1, 0);
ALTER TABLE t_compact RENAME COLUMN p TO pp SETTINGS mutations_sync = 2;
ALTER TABLE t_compact UPDATE pp = 1 WHERE id = 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_compact RENAME COLUMN pp TO p SETTINGS mutations_sync = 2;
SELECT count() FROM t_compact WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_compact WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_compact WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'Update of the indexed column';
CREATE TABLE t_expr (id UInt64, p UInt8, INDEX i id TYPE minmax WHERE p = 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_expr VALUES (1, 1);
ALTER TABLE t_expr RENAME COLUMN p TO pp SETTINGS mutations_sync = 2;
ALTER TABLE t_expr UPDATE id = 2 WHERE id = 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_expr RENAME COLUMN pp TO p SETTINGS mutations_sync = 2;
SELECT count() FROM t_expr WHERE id = 2 AND p = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_expr WHERE id = 2 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_expr WHERE id = 2 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'Delete mutation, which changes the granules';
CREATE TABLE t_delete (id UInt64, p UInt8, INDEX i id TYPE minmax GRANULARITY 1 WHERE p = 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1, packed_skip_index_max_bytes = 0;
INSERT INTO t_delete SELECT number + 1, number + 1 = 4 FROM numbers(6);
ALTER TABLE t_delete RENAME COLUMN p TO pp SETTINGS mutations_sync = 2;
ALTER TABLE t_delete DELETE WHERE id = 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_delete RENAME COLUMN pp TO p SETTINGS mutations_sync = 2;
SELECT count() FROM t_delete WHERE id = 4 AND p = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_delete WHERE id = 4 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_delete WHERE id = 4 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'Update of another column drops the index of the old name';
CREATE TABLE t_other (id UInt64, p UInt8, v UInt8, INDEX i id TYPE minmax GRANULARITY 1 WHERE p = 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1;
INSERT INTO t_other SELECT number, number % 2, 0 FROM numbers(10);
ALTER TABLE t_other RENAME COLUMN p TO pp SETTINGS mutations_sync = 2;
ALTER TABLE t_other UPDATE v = 1 WHERE id = 3 SETTINGS mutations_sync = 2;
ALTER TABLE t_other RENAME COLUMN pp TO p SETTINGS mutations_sync = 2;
SELECT count() FROM t_other WHERE p = 1 AND id = 3 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_other WHERE p = 1 AND id = 3 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_other WHERE p = 1 AND id = 3 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'A rename and its reverse change no data and keep the index';
CREATE TABLE t_rename (id UInt64, p UInt8, INDEX i id TYPE minmax GRANULARITY 1 WHERE p = 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1;
INSERT INTO t_rename SELECT number, number % 2 FROM numbers(10);
ALTER TABLE t_rename RENAME COLUMN p TO pp SETTINGS mutations_sync = 2;
ALTER TABLE t_rename RENAME COLUMN pp TO p SETTINGS mutations_sync = 2;
SELECT count() FROM t_rename WHERE p = 1 AND id = 3 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_rename WHERE p = 1 AND id = 3
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i', max_rows_to_read = 1;

DROP TABLE t_packed;
DROP TABLE t_files;
DROP TABLE t_compact;
DROP TABLE t_expr;
DROP TABLE t_delete;
DROP TABLE t_other;
DROP TABLE t_rename;
