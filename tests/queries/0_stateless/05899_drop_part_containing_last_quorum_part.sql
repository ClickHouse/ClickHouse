-- Tags: replica, no-shared-merge-tree, no-replicated-database, no-async-insert
-- Tag no-shared-merge-tree: SharedMergeTree has no insert quorum.
-- Tag no-replicated-database: two explicit replicas share one ZooKeeper path.
-- Tag no-async-insert: a sequential quorum insert cannot be asynchronous.

DROP TABLE IF EXISTS r1;
DROP TABLE IF EXISTS r2;

CREATE TABLE r1 (part_key Int64, id Int64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') PARTITION BY part_key ORDER BY id SETTINGS remove_empty_parts = 0;
CREATE TABLE r2 (part_key Int64, id Int64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r2') PARTITION BY part_key ORDER BY id SETTINGS remove_empty_parts = 0;

SET insert_keeper_fault_injection_probability = 0;

INSERT INTO r1 SETTINGS insert_quorum = 2, insert_quorum_parallel = 0 VALUES (1, 1);
INSERT INTO r1 SETTINGS insert_quorum = 2, insert_quorum_parallel = 0 VALUES (1, 2);
INSERT INTO r1 VALUES (2, 10);

-- A merged part contains the last part inserted with quorum.
OPTIMIZE TABLE r1 PARTITION 1 FINAL SETTINGS optimize_skip_merged_partitions = 1;
SYSTEM SYNC REPLICA r1;
SYSTEM SYNC REPLICA r2;
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'r2' AND active ORDER BY name;
ALTER TABLE r1 DROP PART '1_0_1_1';
SYSTEM SYNC REPLICA r2;
SELECT * FROM r2 ORDER BY ALL SETTINGS select_sequential_consistency = 1;

-- A mutated (here: emptied) part contains the last part inserted with quorum.
INSERT INTO r1 SETTINGS insert_quorum = 2, insert_quorum_parallel = 0 VALUES (3, 30);
ALTER TABLE r1 DELETE IN PARTITION 3 WHERE 1 SETTINGS mutations_sync = 2;
SYSTEM SYNC REPLICA r2;
SELECT name, rows FROM system.parts WHERE database = currentDatabase() AND table = 'r2' AND partition_id = '3' AND active;
ALTER TABLE r1 DROP PART '3_0_0_0_1';
SYSTEM SYNC REPLICA r2;
SELECT * FROM r2 ORDER BY ALL SETTINGS select_sequential_consistency = 1;

DROP TABLE r1;
DROP TABLE r2;
