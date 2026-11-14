-- A `DEFAULT p IN table` of a column missing in the data of the children of a `Merge` read is evaluated by the
-- readers of the children. The set has to be built once for the whole read also when the children are nested
-- `Merge` tables, or tables reading files, whose readers evaluate the default while parsing.

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_nested_merge_outer_05768;
DROP TABLE IF EXISTS t_nested_merge_1_05768;
DROP TABLE IF EXISTS t_nested_merge_2_05768;
DROP TABLE IF EXISTS t_base_1_05768;
DROP TABLE IF EXISTS t_base_2_05768;
DROP TABLE IF EXISTS t_file_1_05768;
DROP TABLE IF EXISTS t_file_2_05768;
DROP TABLE IF EXISTS keys_05768;

CREATE TABLE keys_05768 (p UInt64) ENGINE = MergeTree ORDER BY p;
INSERT INTO keys_05768 SELECT number * 2 FROM numbers(50);

-- Nested `Merge` tables over `MergeTree` tables whose parts miss the column.
CREATE TABLE t_base_1_05768 (p UInt64) ENGINE = MergeTree ORDER BY p;
CREATE TABLE t_base_2_05768 (p UInt64) ENGINE = MergeTree ORDER BY p;
INSERT INTO t_base_1_05768 SELECT number FROM numbers(100);
INSERT INTO t_base_2_05768 SELECT number FROM numbers(100);
ALTER TABLE t_base_1_05768 ADD COLUMN f UInt8 DEFAULT p IN keys_05768;
ALTER TABLE t_base_2_05768 ADD COLUMN f UInt8 DEFAULT p IN keys_05768;

CREATE TABLE t_nested_merge_1_05768 (p UInt64, f UInt8) ENGINE = Merge(currentDatabase(), '^t_base_1_05768$');
CREATE TABLE t_nested_merge_2_05768 (p UInt64, f UInt8) ENGINE = Merge(currentDatabase(), '^t_base_2_05768$');

SELECT sum(f) FROM merge(currentDatabase(), '^t_nested_merge_[12]_05768$') SETTINGS log_comment = 'default_set_05768_nested_merge';

-- Tables reading files that contain only `p`.
INSERT INTO FUNCTION file(currentDatabase() || '_05768_1.jsonl', 'JSONEachRow', 'p UInt64') SELECT number FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05768_2.jsonl', 'JSONEachRow', 'p UInt64') SELECT number FROM numbers(100) SETTINGS engine_file_truncate_on_insert = 1;
CREATE TABLE t_file_1_05768 AS file(currentDatabase() || '_05768_1.jsonl', 'JSONEachRow', 'p UInt64, f UInt8 DEFAULT p IN ' || currentDatabase() || '.keys_05768');
CREATE TABLE t_file_2_05768 AS file(currentDatabase() || '_05768_2.jsonl', 'JSONEachRow', 'p UInt64, f UInt8 DEFAULT p IN ' || currentDatabase() || '.keys_05768');

SELECT sum(f), sum(p) FROM merge(currentDatabase(), '^t_file_[12]_05768$') SETTINGS log_comment = 'default_set_05768_files';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['SetsBuiltFromSubquery']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'default_set_05768_%'
ORDER BY log_comment;

DROP TABLE t_file_1_05768;
DROP TABLE t_file_2_05768;
DROP TABLE t_nested_merge_1_05768;
DROP TABLE t_nested_merge_2_05768;
DROP TABLE t_base_1_05768;
DROP TABLE t_base_2_05768;
DROP TABLE keys_05768;
