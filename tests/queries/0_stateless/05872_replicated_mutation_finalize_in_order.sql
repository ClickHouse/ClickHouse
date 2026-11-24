-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree

-- A later mutation that finishes first (it touches a partition without parts) must not mark an earlier pending one done.

DROP TABLE IF EXISTS t_in_partition SYNC;
DROP TABLE IF EXISTS t_pruned SYNC;

CREATE TABLE t_in_partition (p UInt8, id UInt64, v UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_in_partition', 'r1') PARTITION BY p ORDER BY id
    SETTINGS finished_mutations_to_keep = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0,
             cleanup_thread_preferred_points_per_iteration = 0;
CREATE TABLE t_pruned (p UInt8, id UInt64, v UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_pruned', 'r1') PARTITION BY p ORDER BY id
    SETTINGS finished_mutations_to_keep = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0,
             cleanup_thread_preferred_points_per_iteration = 0;

INSERT INTO t_in_partition VALUES (1, 1, 10), (2, 2, 20);
INSERT INTO t_pruned VALUES (1, 1, 10), (2, 2, 20);

SYSTEM STOP MERGES t_in_partition;
SYSTEM STOP MERGES t_pruned;

-- The earlier mutation cannot run while merges are stopped.
ALTER TABLE t_in_partition UPDATE v = v + 100 IN PARTITION 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_in_partition DELETE IN PARTITION 3 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_pruned UPDATE v = v + 100 WHERE p = 1 SETTINGS mutations_sync = 0, optimize_mutations_with_partition_pruning = 1;
ALTER TABLE t_pruned DELETE WHERE p = 3 SETTINGS mutations_sync = 0, optimize_mutations_with_partition_pruning = 1;

SELECT sleep(3) FORMAT Null;

SELECT table, mutation_id, is_done, parts_to_do FROM system.mutations
WHERE database = currentDatabase() AND table IN ('t_in_partition', 't_pruned')
ORDER BY table, mutation_id;

SYSTEM START MERGES t_in_partition;
SYSTEM START MERGES t_pruned;
SYSTEM SYNC REPLICA t_in_partition;
SYSTEM SYNC REPLICA t_pruned;

SELECT 'in_partition', p, id, v FROM t_in_partition ORDER BY id;
SELECT 'pruned', p, id, v FROM t_pruned ORDER BY id;

DROP TABLE t_in_partition SYNC;
DROP TABLE t_pruned SYNC;
