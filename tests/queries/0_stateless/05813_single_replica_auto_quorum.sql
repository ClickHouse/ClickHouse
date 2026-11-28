-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- A Replicated database creates additional replicas. This test needs exactly one ReplicatedMergeTree replica.

CREATE TABLE single_replica_auto_quorum (id UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/single_replica_auto_quorum', 'r1')
ORDER BY id;

INSERT INTO single_replica_auto_quorum
SETTINGS async_insert = 0, insert_quorum = 'auto', insert_quorum_parallel = 1, insert_quorum_timeout = 1000
VALUES (1);

SELECT id FROM single_replica_auto_quorum;

DROP TABLE single_replica_auto_quorum SYNC;
