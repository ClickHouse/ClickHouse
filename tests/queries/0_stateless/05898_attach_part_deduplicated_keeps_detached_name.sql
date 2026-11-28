-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- no-replicated-database: FETCH PART reads the table's own Keeper path
-- no-shared-merge-tree: FETCH PART is not supported by SharedMergeTree

-- A deduplicated ATTACH leaves the part in detached/ under the directory it was attached from.

SET insert_keeper_fault_injection_probability = 0;

DROP TABLE IF EXISTS t_attach_dedup SYNC;

CREATE TABLE t_attach_dedup (key Int64, id Int64, val Int64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_attach_dedup', 'r1')
PARTITION BY key ORDER BY id
SETTINGS replicated_deduplication_window = 100;

INSERT INTO t_attach_dedup VALUES (1, 1, 100);
ALTER TABLE t_attach_dedup FETCH PART '1_0_0_0' FROM '/clickhouse/tables/{database}/t_attach_dedup';
ALTER TABLE t_attach_dedup UPDATE val = 200 WHERE id = 1 SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES t_attach_dedup;

ALTER TABLE t_attach_dedup FETCH PART '1_0_0_0_1' FROM '/clickhouse/tables/{database}/t_attach_dedup';
ALTER TABLE t_attach_dedup ATTACH PART '1_0_0_0_1';
ALTER TABLE t_attach_dedup FETCH PART '1_0_0_0_1' FROM '/clickhouse/tables/{database}/t_attach_dedup';

SELECT 'ATTACH PART';
ALTER TABLE t_attach_dedup ATTACH PART '1_0_0_0_1';
SELECT name FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_attach_dedup' ORDER BY name;

SELECT 'ATTACH PARTITION';
ALTER TABLE t_attach_dedup ATTACH PARTITION 1;
SELECT name FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_attach_dedup' ORDER BY name;

SELECT 'ATTACH PART FROM';
ALTER TABLE t_attach_dedup ATTACH PART '1_0_0_0' FROM '1_0_0_0_1';
SELECT name FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_attach_dedup' ORDER BY name;

SELECT count(), sum(val) FROM t_attach_dedup;

DROP TABLE t_attach_dedup SYNC;
