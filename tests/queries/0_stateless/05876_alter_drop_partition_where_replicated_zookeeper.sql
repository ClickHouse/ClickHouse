-- Tags: zookeeper

-- `DROP PARTITION WHERE` and `DETACH PARTITION WHERE` on a `ReplicatedMergeTree` table are replicated.

DROP TABLE IF EXISTS t_dpw_r1 SYNC;
DROP TABLE IF EXISTS t_dpw_r2 SYNC;

CREATE TABLE t_dpw_r1 (d Date, x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_dpw', '1') ORDER BY x PARTITION BY toYYYYMM(d);
CREATE TABLE t_dpw_r2 (d Date, x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_dpw', '2') ORDER BY x PARTITION BY toYYYYMM(d);

INSERT INTO t_dpw_r1 SELECT addMonths(toDate('2025-10-01'), number), number FROM numbers(6);
SYSTEM SYNC REPLICA t_dpw_r2;

ALTER TABLE t_dpw_r1 DROP PARTITION WHERE _partition_value.1 < 202601;
SYSTEM SYNC REPLICA t_dpw_r2;

SELECT 'drop r1', groupArray(partition_id) FROM (SELECT DISTINCT partition_id FROM system.parts WHERE database = currentDatabase() AND table = 't_dpw_r1' AND active ORDER BY partition_id);
SELECT 'drop r2', groupArray(partition_id) FROM (SELECT DISTINCT partition_id FROM system.parts WHERE database = currentDatabase() AND table = 't_dpw_r2' AND active ORDER BY partition_id);

ALTER TABLE t_dpw_r2 DETACH PARTITION WHERE _partition_id LIKE '%2' OR _partition_id = '202603';
SYSTEM SYNC REPLICA t_dpw_r1;

SELECT 'detach r1', groupArray(partition_id) FROM (SELECT DISTINCT partition_id FROM system.parts WHERE database = currentDatabase() AND table = 't_dpw_r1' AND active ORDER BY partition_id);
SELECT 'detach r2', groupArray(partition_id) FROM (SELECT DISTINCT partition_id FROM system.parts WHERE database = currentDatabase() AND table = 't_dpw_r2' AND active ORDER BY partition_id);
SELECT 'detached r1', groupArray(partition_id) FROM (SELECT DISTINCT partition_id FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_dpw_r1' ORDER BY partition_id);
SELECT 'detached r2', groupArray(partition_id) FROM (SELECT DISTINCT partition_id FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_dpw_r2' ORDER BY partition_id);

ALTER TABLE t_dpw_r1 DROP PARTITION WHERE 1;
SYSTEM SYNC REPLICA t_dpw_r2;
SELECT 'all', count() FROM t_dpw_r2;

DROP TABLE t_dpw_r1 SYNC;
DROP TABLE t_dpw_r2 SYNC;
