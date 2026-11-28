-- Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database, no-fasttest
-- The server-global failpoint needs isolation and a build with fault injection enabled.
-- Exactly one registered `ReplicatedMergeTree` replica is required.

CREATE TABLE auto_quorum_recovery (id UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/auto_quorum_recovery', 'r1')
ORDER BY id;
SET async_insert = 0, insert_quorum = 'auto', insert_quorum_parallel = 0,
    insert_keeper_max_retries = 0, insert_keeper_fault_injection_probability = 0;
SYSTEM STOP MERGES auto_quorum_recovery;

-- Interrupt after the part and its 1-of-1 quorum entry have committed.
SYSTEM ENABLE FAILPOINT replicated_merge_tree_insert_quorum_fail_0;
INSERT INTO auto_quorum_recovery VALUES (1); -- { serverError UNKNOWN_STATUS_OF_INSERT }
SYSTEM DISABLE FAILPOINT replicated_merge_tree_insert_quorum_fail_0;
-- A deduplicated retry must finalize the previous entry instead of rejecting it forever.
INSERT INTO auto_quorum_recovery VALUES (1);
INSERT INTO auto_quorum_recovery VALUES (2);
SELECT id FROM auto_quorum_recovery ORDER BY id SETTINGS select_sequential_consistency = 1;

SYSTEM ENABLE FAILPOINT replicated_merge_tree_insert_quorum_fail_0;
INSERT INTO auto_quorum_recovery VALUES (3); -- { serverError UNKNOWN_STATUS_OF_INSERT }
SYSTEM DISABLE FAILPOINT replicated_merge_tree_insert_quorum_fail_0;
-- Restart recovery must finalize the entry even though it already contains the writer.
SYSTEM RESTART REPLICA auto_quorum_recovery;
SELECT id FROM auto_quorum_recovery ORDER BY id SETTINGS select_sequential_consistency = 1;
SELECT count() FROM system.zookeeper
WHERE path = '/clickhouse/tables/' || currentDatabase() || '/auto_quorum_recovery/quorum' AND name = 'status';

SET insert_quorum_parallel = 1;
SYSTEM ENABLE FAILPOINT replicated_merge_tree_insert_quorum_fail_0;
INSERT INTO auto_quorum_recovery VALUES (4); -- { serverError UNKNOWN_STATUS_OF_INSERT }
SYSTEM DISABLE FAILPOINT replicated_merge_tree_insert_quorum_fail_0;
SYSTEM RESTART REPLICA auto_quorum_recovery;
SELECT count() FROM system.zookeeper
WHERE path = '/clickhouse/tables/' || currentDatabase() || '/auto_quorum_recovery/quorum/parallel';
SELECT id FROM auto_quorum_recovery ORDER BY id;
DROP TABLE auto_quorum_recovery SYNC;
