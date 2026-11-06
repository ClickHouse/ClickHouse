-- Tags: zookeeper, no-parallel, no-shared-merge-tree, no-fasttest
-- Regression test for CHECK TABLE on a ReplicatedMergeTree replica that attached without ever
-- reaching Keeper (e.g. restart during Keeper outage). `current_zookeeper` stays null
-- (StorageReplicatedMergeTree.cpp:535), so `findLocalPart` throws NO_ZOOKEEPER from getZooKeeper()
-- (StorageReplicatedMergeTree.cpp:366). That error is not retryable, so checkDataNext turned it into
-- a failed per-part result and CHECK TABLE ... check_query_single_value_result = 1 returned 0 (false
-- corruption alarm) instead of failing. The fix makes checkDataNext throw ABORTED when
-- tryGetZooKeeper() is null, like the shutdown guard.
-- Related: https://github.com/ClickHouse/ClickHouse/issues/124457
-- The failpoint is server-global, hence no-parallel. It lives in StorageReplicatedMergeTree::checkDataNext,
-- hence no-shared-merge-tree.

SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_05565 SYNC;

CREATE TABLE t_05565 (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_05565', 'r1')
ORDER BY a;

INSERT INTO t_05565 VALUES (1);
INSERT INTO t_05565 VALUES (2);

-- Healthy parts check out.
CHECK TABLE t_05565;

-- Simulate a replica that never got a Keeper session: the check must fail, not return 0.
SYSTEM ENABLE FAILPOINT check_table_inject_no_keeper;

CHECK TABLE t_05565; -- { serverError ABORTED }
SYSTEM DISABLE FAILPOINT check_table_inject_no_keeper;

-- Healthy again after the failpoint is consumed (ONCE).
CHECK TABLE t_05565;

DROP TABLE t_05565 SYNC;

-- Empty table: no part to check, so the final "no part left" call must not fail even with the failpoint.
DROP TABLE IF EXISTS t_05565_empty SYNC;

CREATE TABLE t_05565_empty (a UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_05565_empty', 'r1')
ORDER BY a;

SYSTEM ENABLE FAILPOINT check_table_inject_no_keeper;
CHECK TABLE t_05565_empty;
SYSTEM DISABLE FAILPOINT check_table_inject_no_keeper;

DROP TABLE t_05565_empty SYNC;
