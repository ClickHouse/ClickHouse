-- Tags: replica, no-shared-merge-tree, no-replicated-database, no-async-insert
-- Tag no-shared-merge-tree: SharedMergeTree has no insert quorum.
-- Tag no-replicated-database: two explicit replicas share one ZooKeeper path.
-- Tag no-async-insert: a sequential quorum insert cannot be asynchronous.

DROP TABLE IF EXISTS dst_r1;
DROP TABLE IF EXISTS dst_r2;
DROP TABLE IF EXISTS src;

CREATE TABLE dst_r1 (part_key Int64, id Int64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst', 'r1') PARTITION BY part_key ORDER BY id;
CREATE TABLE dst_r2 (part_key Int64, id Int64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/dst', 'r2') PARTITION BY part_key ORDER BY id;
CREATE TABLE src (part_key Int64, id Int64) ENGINE = MergeTree PARTITION BY part_key ORDER BY id;

SET insert_keeper_fault_injection_probability = 0;

INSERT INTO dst_r1 SETTINGS insert_quorum = 2, insert_quorum_parallel = 0 VALUES (1, 1);
INSERT INTO dst_r1 SETTINGS insert_quorum = 2, insert_quorum_parallel = 0 VALUES (2, 2);
INSERT INTO src VALUES (1, 10), (2, 20);

ALTER TABLE dst_r1 REPLACE PARTITION 1 FROM src SETTINGS alter_sync = 2;

SELECT * FROM dst_r1 ORDER BY ALL SETTINGS select_sequential_consistency = 1;
SELECT * FROM dst_r2 ORDER BY ALL SETTINGS select_sequential_consistency = 1;

-- The partition is replaced even when the wait for an inactive replica fails.
DETACH TABLE dst_r2;
ALTER TABLE dst_r1 REPLACE PARTITION 2 FROM src SETTINGS alter_sync = 2, replication_wait_for_inactive_replica_timeout = 0; -- { serverError UNFINISHED }
SELECT * FROM dst_r1 ORDER BY ALL SETTINGS select_sequential_consistency = 1;

ATTACH TABLE dst_r2;
SYSTEM SYNC REPLICA dst_r2;
SELECT * FROM dst_r2 ORDER BY ALL SETTINGS select_sequential_consistency = 1;

DROP TABLE dst_r1;
DROP TABLE dst_r2;
DROP TABLE src;
