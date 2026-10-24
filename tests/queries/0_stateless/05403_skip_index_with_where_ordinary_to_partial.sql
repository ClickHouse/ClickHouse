-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas

-- A part detached while an ordinary index is redefined as a partial one keeps the ordinary files.
-- A data mutation must drop them, and MATERIALIZE INDEX must not carry them over.

SET allow_experimental_partial_skip_indexes = 1;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET mutations_sync = 2;

DROP TABLE IF EXISTS t_ord_files;
DROP TABLE IF EXISTS t_ord_set;
DROP TABLE IF EXISTS t_ord_packed;
DROP TABLE IF EXISTS t_ord_materialize;

SELECT 'ordinary to partial, mutate, back to ordinary: minmax, own files';
CREATE TABLE t_ord_files (id UInt64, p UInt8, INDEX i id TYPE minmax)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0, index_granularity = 1;
INSERT INTO t_ord_files VALUES (1, 1);
ALTER TABLE t_ord_files DETACH PARTITION tuple();
ALTER TABLE t_ord_files DROP INDEX i;
ALTER TABLE t_ord_files ADD INDEX i id TYPE minmax WHERE p = 1;
ALTER TABLE t_ord_files ATTACH PARTITION tuple();
ALTER TABLE t_ord_files UPDATE id = 2 WHERE id = 1;
ALTER TABLE t_ord_files DETACH PARTITION tuple();
ALTER TABLE t_ord_files DROP INDEX i;
ALTER TABLE t_ord_files ADD INDEX i id TYPE minmax;
ALTER TABLE t_ord_files ATTACH PARTITION tuple();
SELECT count() FROM t_ord_files WHERE id = 2 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ord_files WHERE id = 2 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_ord_files WHERE id = 2 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'ordinary to partial, mutate, back to ordinary: set, own files';
CREATE TABLE t_ord_set (id UInt64, p UInt8, INDEX i id TYPE set(10))
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0, index_granularity = 1;
INSERT INTO t_ord_set VALUES (1, 1);
ALTER TABLE t_ord_set DETACH PARTITION tuple();
ALTER TABLE t_ord_set DROP INDEX i;
ALTER TABLE t_ord_set ADD INDEX i id TYPE set(10) WHERE p = 1;
ALTER TABLE t_ord_set ATTACH PARTITION tuple();
ALTER TABLE t_ord_set UPDATE id = 2 WHERE id = 1;
ALTER TABLE t_ord_set DETACH PARTITION tuple();
ALTER TABLE t_ord_set DROP INDEX i;
ALTER TABLE t_ord_set ADD INDEX i id TYPE set(10);
ALTER TABLE t_ord_set ATTACH PARTITION tuple();
SELECT count() FROM t_ord_set WHERE id = 2 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ord_set WHERE id = 2 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_ord_set WHERE id = 2 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'ordinary to partial, mutate, back to ordinary: minmax, packed';
CREATE TABLE t_ord_packed (id UInt64, p UInt8, INDEX i id TYPE minmax)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1;
INSERT INTO t_ord_packed VALUES (1, 1);
ALTER TABLE t_ord_packed DETACH PARTITION tuple();
ALTER TABLE t_ord_packed DROP INDEX i;
ALTER TABLE t_ord_packed ADD INDEX i id TYPE minmax WHERE p = 1;
ALTER TABLE t_ord_packed ATTACH PARTITION tuple();
ALTER TABLE t_ord_packed UPDATE id = 2 WHERE id = 1;
ALTER TABLE t_ord_packed DETACH PARTITION tuple();
ALTER TABLE t_ord_packed DROP INDEX i;
ALTER TABLE t_ord_packed ADD INDEX i id TYPE minmax;
ALTER TABLE t_ord_packed ATTACH PARTITION tuple();
SELECT count() FROM t_ord_packed WHERE id = 2 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_ord_packed WHERE id = 2 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_ord_packed WHERE id = 2 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'ordinary to partial, MATERIALIZE INDEX';
CREATE TABLE t_ord_materialize (id UInt64, p UInt8, INDEX i id TYPE minmax)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0, index_granularity = 1;
INSERT INTO t_ord_materialize VALUES (1, 1), (2, 0), (3, 1);
ALTER TABLE t_ord_materialize DETACH PARTITION tuple();
ALTER TABLE t_ord_materialize DROP INDEX i;
ALTER TABLE t_ord_materialize ADD INDEX i id TYPE minmax WHERE p = 1;
ALTER TABLE t_ord_materialize ATTACH PARTITION tuple();
ALTER TABLE t_ord_materialize MATERIALIZE INDEX i;
SELECT is_done, latest_fail_reason = '' FROM system.mutations WHERE database = currentDatabase() AND table = 't_ord_materialize';
SELECT count() FROM t_ord_materialize WHERE id = 3 AND p = 1;
SELECT count() FROM t_ord_materialize WHERE id = 2 AND p = 1;

DROP TABLE t_ord_files;
DROP TABLE t_ord_set;
DROP TABLE t_ord_packed;
DROP TABLE t_ord_materialize;
