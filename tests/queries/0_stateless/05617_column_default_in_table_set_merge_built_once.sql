-- A `DEFAULT p IN table` of a column missing in the parts of the children of a `Merge` read is evaluated by the
-- readers of the children. The set of the default has to be built once for the whole read, not once per child table.

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_default_set_merge_1;
DROP TABLE IF EXISTS t_default_set_merge_2;
DROP TABLE IF EXISTS t_default_set_merge_3;
DROP TABLE IF EXISTS keys_default_set_merge;

CREATE TABLE keys_default_set_merge (p UInt64) ENGINE = MergeTree ORDER BY p;
INSERT INTO keys_default_set_merge SELECT number * 2 FROM numbers(50);

CREATE TABLE t_default_set_merge_1 (p UInt64) ENGINE = MergeTree ORDER BY p;
CREATE TABLE t_default_set_merge_2 (p UInt64) ENGINE = MergeTree ORDER BY p;
CREATE TABLE t_default_set_merge_3 (p UInt64) ENGINE = MergeTree ORDER BY p;
INSERT INTO t_default_set_merge_1 SELECT number FROM numbers(100);
INSERT INTO t_default_set_merge_2 SELECT number FROM numbers(100);
INSERT INTO t_default_set_merge_3 SELECT number FROM numbers(100);

ALTER TABLE t_default_set_merge_1 ADD COLUMN f UInt8 DEFAULT p IN keys_default_set_merge;
ALTER TABLE t_default_set_merge_2 ADD COLUMN f UInt8 DEFAULT p IN keys_default_set_merge;
ALTER TABLE t_default_set_merge_3 ADD COLUMN f UInt8 DEFAULT p IN keys_default_set_merge;

SELECT sum(f) FROM merge(currentDatabase(), '^t_default_set_merge_') SETTINGS log_comment = 'default_set_merge_default';
SELECT sum(p IN keys_default_set_merge) FROM merge(currentDatabase(), '^t_default_set_merge_') SETTINGS log_comment = 'default_set_merge_direct';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['SetsBuiltFromSubquery']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'default_set_merge_%'
ORDER BY log_comment;

DROP TABLE t_default_set_merge_1;
DROP TABLE t_default_set_merge_2;
DROP TABLE t_default_set_merge_3;
DROP TABLE keys_default_set_merge;
