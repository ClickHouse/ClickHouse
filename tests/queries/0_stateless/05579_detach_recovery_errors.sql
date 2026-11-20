-- Tags: no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
-- The failpoint injects a server-global recovery failure.

CREATE TABLE t_recovery_load (p UInt8, k UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS remove_empty_parts = 0, old_parts_lifetime = 3600;
INSERT INTO t_recovery_load SETTINGS async_insert = 0 VALUES (1, 1), (1, 2), (1, 3);
ALTER TABLE t_recovery_load DETACH PARTITION 1;
DETACH TABLE t_recovery_load;
SYSTEM ENABLE FAILPOINT mt_throw_before_outdating_empty_parts;
ATTACH TABLE t_recovery_load; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT mt_throw_before_outdating_empty_parts;
EXISTS TABLE t_recovery_load;
ATTACH TABLE t_recovery_load;
ALTER TABLE t_recovery_load ATTACH PARTITION 1;
SELECT count(), sum(k) FROM t_recovery_load;
DROP TABLE t_recovery_load;

CREATE TABLE t_recovery_readonly (p UInt8, k UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS remove_empty_parts = 0, old_parts_lifetime = 3600;
INSERT INTO t_recovery_readonly SETTINGS async_insert = 0 VALUES (1, 1), (1, 2), (1, 3);
ALTER TABLE t_recovery_readonly DETACH PARTITION 1;
ALTER TABLE t_recovery_readonly MODIFY SETTING table_readonly = 1;
DETACH TABLE t_recovery_readonly;
ATTACH TABLE t_recovery_readonly;
SYSTEM ENABLE FAILPOINT mt_throw_before_outdating_empty_parts;
ALTER TABLE t_recovery_readonly MODIFY SETTING table_readonly = 0; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT mt_throw_before_outdating_empty_parts;
INSERT INTO t_recovery_readonly SETTINGS async_insert = 0 VALUES (1, 4); -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }
OPTIMIZE TABLE t_recovery_readonly FINAL; -- { serverError TABLE_IS_PERMANENTLY_READ_ONLY }
ALTER TABLE t_recovery_readonly MODIFY SETTING table_readonly = 0;
INSERT INTO t_recovery_readonly SETTINGS async_insert = 0 VALUES (1, 4);
OPTIMIZE TABLE t_recovery_readonly PARTITION 1 FINAL;
ALTER TABLE t_recovery_readonly DETACH PARTITION 1;
ALTER TABLE t_recovery_readonly ATTACH PARTITION 1;
SELECT count(), sum(k) FROM t_recovery_readonly;
DROP TABLE t_recovery_readonly;