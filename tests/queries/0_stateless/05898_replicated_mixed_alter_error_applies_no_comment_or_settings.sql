-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database: a mixed replicated and non-replicated ALTER is rejected there.
-- Tag no-shared-merge-tree: the test covers the ALTER path of ReplicatedMergeTree itself.

-- A failed ALTER that mixes a replicated change with MODIFY COMMENT / MODIFY SETTING must not
-- apply the comment or the setting, neither in memory nor in the table metadata file.

DROP TABLE IF EXISTS t SYNC;
DROP TABLE IF EXISTS r1 SYNC;
DROP TABLE IF EXISTS r2 SYNC;

SELECT 'too many mutations';
CREATE TABLE t (part_key Int64, id Int64, val Int64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mixed_alter', 'r1')
PARTITION BY part_key ORDER BY (part_key, id)
SETTINGS number_of_mutations_to_throw = 1 COMMENT 'original';
INSERT INTO t VALUES (1, 1, 100);
SYSTEM STOP MERGES t;
ALTER TABLE t UPDATE val = val + 1 WHERE 1 SETTINGS mutations_sync = 0, alter_update_mode = 'heavy';
SYSTEM SYNC REPLICA t PULL;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't' AND NOT is_done;
ALTER TABLE t MODIFY COLUMN val String, MODIFY COMMENT 'leaked', MODIFY SETTING check_delay_period = 77; -- { serverError TOO_MANY_MUTATIONS }
SELECT comment, position(create_table_query, 'check_delay_period = 77') FROM system.tables WHERE database = currentDatabase() AND name = 't';
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't' AND name = 'val';
DETACH TABLE t;
ATTACH TABLE t;
SELECT comment, position(create_table_query, 'check_delay_period = 77') FROM system.tables WHERE database = currentDatabase() AND name = 't';

SELECT 'replica not up to date';
CREATE TABLE r1 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mixed_alter_lag', 'r1') ORDER BY k COMMENT 'original';
CREATE TABLE r2 (k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_mixed_alter_lag', 'r2') ORDER BY k COMMENT 'original';
SYSTEM STOP PULLING REPLICATION LOG r2;
ALTER TABLE r1 ADD COLUMN a UInt8 SETTINGS alter_sync = 1;
ALTER TABLE r2 ADD COLUMN b UInt8, MODIFY COMMENT 'leaked', MODIFY SETTING check_delay_period = 77; -- { serverError CANNOT_ASSIGN_ALTER }
SELECT comment, position(create_table_query, 'check_delay_period = 77') FROM system.tables WHERE database = currentDatabase() AND name = 'r2';
SYSTEM START PULLING REPLICATION LOG r2;
SYSTEM SYNC REPLICA r2;
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'r2' ORDER BY name;
DETACH TABLE r2;
ATTACH TABLE r2;
SELECT comment, position(create_table_query, 'check_delay_period = 77') FROM system.tables WHERE database = currentDatabase() AND name = 'r2';

SELECT 'invalid setting';
ALTER TABLE r1 ADD COLUMN c UInt8, MODIFY SETTING number_of_free_entries_in_pool_to_execute_mutation = 1000000; -- { serverError BAD_ARGUMENTS }
SYSTEM SYNC REPLICA r1;
SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 'r1' ORDER BY name;
SELECT position(create_table_query, 'number_of_free_entries_in_pool_to_execute_mutation') FROM system.tables WHERE database = currentDatabase() AND name = 'r1';

DROP TABLE t SYNC;
DROP TABLE r1 SYNC;
DROP TABLE r2 SYNC;
