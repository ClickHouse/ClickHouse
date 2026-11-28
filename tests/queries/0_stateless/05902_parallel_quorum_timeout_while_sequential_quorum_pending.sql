-- Tags: replica, no-shared-merge-tree, no-replicated-database, no-async-insert
-- Tag no-shared-merge-tree: SharedMergeTree has no insert quorum.
-- Tag no-replicated-database: three explicit replicas share one ZooKeeper path.
-- Tag no-async-insert: a sequential quorum insert cannot be asynchronous.

DROP TABLE IF EXISTS t_r1;
DROP TABLE IF EXISTS t_r2;
DROP TABLE IF EXISTS t_r3;

CREATE TABLE t_r1 (s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') ORDER BY s;
CREATE TABLE t_r2 (s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r2') ORDER BY s;
CREATE TABLE t_r3 (s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r3') ORDER BY s;

SET insert_keeper_fault_injection_probability = 0;

SYSTEM STOP FETCHES t_r1;
SYSTEM STOP FETCHES t_r2;
SYSTEM STOP FETCHES t_r3;
SYSTEM STOP REPLICATED SENDS t_r1;

-- /quorum/status holds all_0_0_0, /quorum/parallel holds all_1_1_0.
INSERT INTO t_r1 SETTINGS insert_quorum = 2, insert_quorum_parallel = 0, insert_quorum_timeout = 0 VALUES ('sequential'); -- { serverError UNKNOWN_STATUS_OF_INSERT }
INSERT INTO t_r2 SETTINGS insert_quorum = 2, insert_quorum_parallel = 1, insert_quorum_timeout = 0, insert_deduplicate = 1 VALUES ('parallel'); -- { serverError UNKNOWN_STATUS_OF_INSERT }
SELECT name FROM system.zookeeper WHERE path = '/clickhouse/tables/' || currentDatabase() || '/t/quorum' AND name = 'status';
SELECT name FROM system.zookeeper WHERE path = '/clickhouse/tables/' || currentDatabase() || '/t/quorum/parallel';

-- No active replica has all_1_1_0, so r3 marks its quorum as failed.
DETACH TABLE t_r2;
SYSTEM START FETCHES t_r3;
SET receive_timeout = 60;
SYSTEM SYNC REPLICA t_r3 LIGHTWEIGHT FROM 'r2';
SELECT name FROM system.zookeeper WHERE path = '/clickhouse/tables/' || currentDatabase() || '/t/quorum/failed_parts';
SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/' || currentDatabase() || '/t/quorum/parallel';
SELECT name FROM system.zookeeper WHERE path = '/clickhouse/tables/' || currentDatabase() || '/t/quorum' AND name = 'status';

-- Retrying the failed insert on another replica writes it.
INSERT INTO t_r3 SETTINGS insert_deduplicate = 1 VALUES ('parallel');
SELECT * FROM t_r3 ORDER BY ALL;

-- r2 detaches its copy of the failed part, and every replica ends with each row once.
ATTACH TABLE t_r2;
SYSTEM START REPLICATED SENDS t_r1;
SYSTEM START FETCHES t_r1;
SYSTEM SYNC REPLICA t_r1;
SYSTEM SYNC REPLICA t_r2;
SYSTEM SYNC REPLICA t_r3;
SELECT reason FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_r2';
SELECT _table, s FROM merge(currentDatabase(), '^t_r[123]$') ORDER BY ALL;

DROP TABLE t_r1;
DROP TABLE t_r2;
DROP TABLE t_r3;
