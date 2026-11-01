-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas

-- A part detached while its partial index is redefined keeps the files of the old definition. A mutation that changes
-- data must drop them, or they describe old data once the old definition is restored and its marker matches again.

SET allow_experimental_partial_skip_indexes = 1;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_other_files;
DROP TABLE IF EXISTS t_other_packed;
DROP TABLE IF EXISTS t_other_compact;
DROP TABLE IF EXISTS t_ordinary_files;
DROP TABLE IF EXISTS t_ordinary_packed;
DROP TABLE IF EXISTS t_none_files;
DROP TABLE IF EXISTS t_none_packed;
DROP TABLE IF EXISTS t_patch;

SELECT 'partial to another partial, own files';
CREATE TABLE t_other_files (id UInt64, p UInt8, q UInt8, INDEX i id TYPE minmax WHERE p = 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0;
INSERT INTO t_other_files VALUES (1, 0, 1);
ALTER TABLE t_other_files DETACH PARTITION tuple();
ALTER TABLE t_other_files DROP INDEX i;
ALTER TABLE t_other_files ADD INDEX i id TYPE minmax WHERE q = 1;
ALTER TABLE t_other_files ATTACH PARTITION tuple();
ALTER TABLE t_other_files UPDATE p = 1 WHERE id = 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_other_files DETACH PARTITION tuple();
ALTER TABLE t_other_files DROP INDEX i;
ALTER TABLE t_other_files ADD INDEX i id TYPE minmax WHERE p = 1;
ALTER TABLE t_other_files ATTACH PARTITION tuple();
SELECT count() FROM t_other_files WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_other_files WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_other_files WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'partial to another partial, packed archive';
CREATE TABLE t_other_packed (id UInt64, p UInt8, q UInt8, INDEX i id TYPE minmax WHERE p = 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 1048576;
INSERT INTO t_other_packed VALUES (1, 0, 1);
ALTER TABLE t_other_packed DETACH PARTITION tuple();
ALTER TABLE t_other_packed DROP INDEX i;
ALTER TABLE t_other_packed ADD INDEX i id TYPE minmax WHERE q = 1;
ALTER TABLE t_other_packed ATTACH PARTITION tuple();
ALTER TABLE t_other_packed UPDATE p = 1 WHERE id = 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_other_packed DETACH PARTITION tuple();
ALTER TABLE t_other_packed DROP INDEX i;
ALTER TABLE t_other_packed ADD INDEX i id TYPE minmax WHERE p = 1;
ALTER TABLE t_other_packed ATTACH PARTITION tuple();
SELECT count() FROM t_other_packed WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_other_packed WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_other_packed WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'partial to another partial, Compact part';
CREATE TABLE t_other_compact (id UInt64, p UInt8, q UInt8, INDEX i id TYPE minmax WHERE p = 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = '100G', packed_skip_index_max_bytes = 0;
INSERT INTO t_other_compact VALUES (1, 0, 1);
ALTER TABLE t_other_compact DETACH PARTITION tuple();
ALTER TABLE t_other_compact DROP INDEX i;
ALTER TABLE t_other_compact ADD INDEX i id TYPE minmax WHERE q = 1;
ALTER TABLE t_other_compact ATTACH PARTITION tuple();
ALTER TABLE t_other_compact UPDATE p = 1 WHERE id = 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_other_compact DETACH PARTITION tuple();
ALTER TABLE t_other_compact DROP INDEX i;
ALTER TABLE t_other_compact ADD INDEX i id TYPE minmax WHERE p = 1;
ALTER TABLE t_other_compact ATTACH PARTITION tuple();
SELECT count() FROM t_other_compact WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_other_compact WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_other_compact WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'partial to ordinary, own files';
CREATE TABLE t_ordinary_files (id UInt64, p UInt8, q UInt8, INDEX i id TYPE minmax WHERE p = 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0;
INSERT INTO t_ordinary_files VALUES (1, 0, 1);
ALTER TABLE t_ordinary_files DETACH PARTITION tuple();
ALTER TABLE t_ordinary_files DROP INDEX i;
ALTER TABLE t_ordinary_files ADD INDEX i id TYPE minmax;
ALTER TABLE t_ordinary_files ATTACH PARTITION tuple();
ALTER TABLE t_ordinary_files UPDATE p = 1 WHERE id = 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_ordinary_files DETACH PARTITION tuple();
ALTER TABLE t_ordinary_files DROP INDEX i;
ALTER TABLE t_ordinary_files ADD INDEX i id TYPE minmax WHERE p = 1;
ALTER TABLE t_ordinary_files ATTACH PARTITION tuple();
SELECT count() FROM t_ordinary_files WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ordinary_files WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_ordinary_files WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'partial to ordinary, packed archive';
CREATE TABLE t_ordinary_packed (id UInt64, p UInt8, q UInt8, INDEX i id TYPE minmax WHERE p = 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 1048576;
INSERT INTO t_ordinary_packed VALUES (1, 0, 1);
ALTER TABLE t_ordinary_packed DETACH PARTITION tuple();
ALTER TABLE t_ordinary_packed DROP INDEX i;
ALTER TABLE t_ordinary_packed ADD INDEX i id TYPE minmax;
ALTER TABLE t_ordinary_packed ATTACH PARTITION tuple();
ALTER TABLE t_ordinary_packed UPDATE p = 1 WHERE id = 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_ordinary_packed DETACH PARTITION tuple();
ALTER TABLE t_ordinary_packed DROP INDEX i;
ALTER TABLE t_ordinary_packed ADD INDEX i id TYPE minmax WHERE p = 1;
ALTER TABLE t_ordinary_packed ATTACH PARTITION tuple();
SELECT count() FROM t_ordinary_packed WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ordinary_packed WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_ordinary_packed WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'partial dropped, then added again, own files';
CREATE TABLE t_none_files (id UInt64, p UInt8, INDEX i id TYPE minmax WHERE p = 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0;
INSERT INTO t_none_files VALUES (1, 0);
ALTER TABLE t_none_files DETACH PARTITION tuple();
ALTER TABLE t_none_files DROP INDEX i;
ALTER TABLE t_none_files ATTACH PARTITION tuple();
ALTER TABLE t_none_files UPDATE p = 1 WHERE id = 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_none_files ADD INDEX i id TYPE minmax WHERE p = 1;
SELECT count() FROM t_none_files WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_none_files WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_none_files WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'partial dropped, then added again, packed archive';
CREATE TABLE t_none_packed (id UInt64, p UInt8, INDEX i id TYPE minmax WHERE p = 1)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 1048576;
INSERT INTO t_none_packed VALUES (1, 0);
ALTER TABLE t_none_packed DETACH PARTITION tuple();
ALTER TABLE t_none_packed DROP INDEX i;
ALTER TABLE t_none_packed ATTACH PARTITION tuple();
ALTER TABLE t_none_packed UPDATE p = 1 WHERE id = 1 SETTINGS mutations_sync = 2;
ALTER TABLE t_none_packed ADD INDEX i id TYPE minmax WHERE p = 1;
SELECT count() FROM t_none_packed WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_none_packed WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_none_packed WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'partial to another partial, lightweight UPDATE applied by APPLY PATCHES';
CREATE TABLE t_patch (id UInt64, p UInt8, q UInt8, INDEX i id TYPE minmax WHERE p = 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0, enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_patch VALUES (1, 0, 1);
ALTER TABLE t_patch DETACH PARTITION tuple();
ALTER TABLE t_patch DROP INDEX i;
ALTER TABLE t_patch ADD INDEX i id TYPE minmax WHERE q = 1;
ALTER TABLE t_patch ATTACH PARTITION tuple();
UPDATE t_patch SET p = 1 WHERE id = 1;
ALTER TABLE t_patch APPLY PATCHES SETTINGS mutations_sync = 2;
ALTER TABLE t_patch DETACH PARTITION tuple();
ALTER TABLE t_patch DROP INDEX i;
ALTER TABLE t_patch ADD INDEX i id TYPE minmax WHERE p = 1;
ALTER TABLE t_patch ATTACH PARTITION tuple();
SELECT count() FROM t_patch WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_patch WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_patch WHERE id = 1 AND p = 1 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

DROP TABLE t_other_files;
DROP TABLE t_other_packed;
DROP TABLE t_other_compact;
DROP TABLE t_ordinary_files;
DROP TABLE t_ordinary_packed;
DROP TABLE t_none_files;
DROP TABLE t_none_packed;
DROP TABLE t_patch;
