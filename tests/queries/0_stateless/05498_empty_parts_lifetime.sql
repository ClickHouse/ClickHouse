-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database: a Replicated database refuses the explicit ZooKeeper path and replica name of t_epl_rmt.
-- no-shared-merge-tree: the test is about the empty part cleanup of MergeTree and ReplicatedMergeTree.

-- old_parts_lifetime is pinned so that an outdated empty part stays visible in system.parts.

DROP TABLE IF EXISTS t_epl_truncate;
DROP TABLE IF EXISTS t_epl_drop_partition;
DROP TABLE IF EXISTS t_epl_rmt;

-- The empty part left by TRUNCATE stays active.
CREATE TABLE t_epl_truncate (k UInt32) ENGINE = MergeTree ORDER BY k
SETTINGS old_parts_lifetime = 86400, empty_parts_lifetime = 86400;
INSERT INTO t_epl_truncate SELECT number FROM numbers(100) SETTINGS async_insert = 0;
TRUNCATE TABLE t_epl_truncate;
SELECT 'truncate', active FROM system.parts WHERE database = currentDatabase() AND table = 't_epl_truncate' AND rows = 0;
SELECT count() FROM t_epl_truncate;

CREATE TABLE t_epl_drop_partition (p UInt8, k UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS old_parts_lifetime = 86400;
INSERT INTO t_epl_drop_partition SETTINGS async_insert = 0 VALUES (1, 1), (2, 2), (3, 3);

-- With the default 0, the empty part is removed at once.
ALTER TABLE t_epl_drop_partition DROP PARTITION 1;
SELECT 'default', partition, active FROM system.parts WHERE database = currentDatabase() AND table = 't_epl_drop_partition' AND rows = 0 ORDER BY partition;

-- A young empty part stays active.
ALTER TABLE t_epl_drop_partition MODIFY SETTING empty_parts_lifetime = 86400;
ALTER TABLE t_epl_drop_partition DROP PARTITION 2;
SELECT 'young', partition, active FROM system.parts WHERE database = currentDatabase() AND table = 't_epl_drop_partition' AND rows = 0 ORDER BY partition;

-- The empty part left by a mutation of ReplicatedMergeTree stays active.
CREATE TABLE t_epl_rmt (k UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_epl_rmt', 'r1') ORDER BY k
SETTINGS empty_parts_lifetime = 86400, old_parts_lifetime = 86400, cleanup_delay_period = 1, max_cleanup_delay_period = 1, cleanup_delay_period_random_add = 0;
INSERT INTO t_epl_rmt SELECT number FROM numbers(10) SETTINGS async_insert = 0;
ALTER TABLE t_epl_rmt DELETE WHERE 1 SETTINGS mutations_sync = 2;
-- Lets the cleanup thread of t_epl_rmt run, and ages the empty part of partition 2.
SELECT sleep(2) FORMAT Null;
SELECT 'replicated', active FROM system.parts WHERE database = currentDatabase() AND table = 't_epl_rmt' AND rows = 0;

-- An empty part older than empty_parts_lifetime is removed.
ALTER TABLE t_epl_drop_partition MODIFY SETTING empty_parts_lifetime = 1;
ALTER TABLE t_epl_drop_partition DROP PARTITION 3;
-- The empty part of partition 3 can be younger or older than 1 second, so it is not checked.
SELECT 'old', partition, active FROM system.parts WHERE database = currentDatabase() AND table = 't_epl_drop_partition' AND rows = 0 AND partition != '3' ORDER BY partition;

DROP TABLE t_epl_truncate;
DROP TABLE t_epl_drop_partition;
DROP TABLE t_epl_rmt;
