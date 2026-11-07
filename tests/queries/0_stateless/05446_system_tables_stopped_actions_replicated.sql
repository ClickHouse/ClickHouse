-- Tags: zookeeper

CREATE TABLE stopped_actions_replica (x UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/stopped_actions_replica', 'r1') ORDER BY x;

SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_replica';
SYSTEM STOP TTL MERGES stopped_actions_replica;
SYSTEM STOP REPLICATION QUEUES stopped_actions_replica;
SYSTEM STOP REPLICATED SENDS stopped_actions_replica;
SYSTEM STOP PULLING REPLICATION LOG stopped_actions_replica;
SYSTEM STOP MOVES stopped_actions_replica;
SYSTEM STOP MERGES stopped_actions_replica;
SYSTEM STOP FETCHES stopped_actions_replica;
SYSTEM STOP CLEANUP stopped_actions_replica;
SYSTEM STOP FETCHES stopped_actions_replica;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_replica';

SYSTEM START REPLICATION QUEUES stopped_actions_replica;
SYSTEM START REPLICATED SENDS stopped_actions_replica;
SYSTEM START PULLING REPLICATION LOG stopped_actions_replica;
SYSTEM START FETCHES stopped_actions_replica;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_replica';
SYSTEM START CLEANUP stopped_actions_replica;
SYSTEM START MERGES stopped_actions_replica;
SYSTEM START MOVES stopped_actions_replica;
SYSTEM START TTL MERGES stopped_actions_replica;
SELECT stopped_actions FROM system.tables WHERE database = currentDatabase() AND name = 'stopped_actions_replica';

DROP TABLE stopped_actions_replica;
