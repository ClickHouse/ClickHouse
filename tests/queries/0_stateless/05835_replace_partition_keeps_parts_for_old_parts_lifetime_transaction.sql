-- Tags: no-ordinary-database, no-fasttest, no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database, no-shared-merge-tree: replicated and shared tables remove the replaced parts at once

DROP TABLE IF EXISTS t_txn_dst;
DROP TABLE IF EXISTS t_txn_src;

-- A long old_parts_lifetime, so that a kept part cannot age out while the test runs.
CREATE TABLE t_txn_dst (k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS old_parts_lifetime = 3600;
CREATE TABLE t_txn_src (k UInt32) ENGINE = MergeTree ORDER BY k;

SYSTEM STOP MERGES t_txn_dst;

INSERT INTO t_txn_dst VALUES (1), (2), (3);
INSERT INTO t_txn_src VALUES (4), (5);

BEGIN TRANSACTION;
ALTER TABLE t_txn_dst REPLACE PARTITION tuple() FROM t_txn_src;
COMMIT;

SELECT k FROM t_txn_dst ORDER BY k;
SELECT active, remove_time > toDateTime(0) FROM system.parts WHERE database = currentDatabase() AND table = 't_txn_dst' AND rows = 3;

DROP TABLE t_txn_dst;
DROP TABLE t_txn_src;
