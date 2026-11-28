-- Tags: no-ordinary-database, no-fasttest, no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database, no-shared-merge-tree: replicated and shared tables do not keep the truncated parts

DROP TABLE IF EXISTS t_txn_default;
DROP TABLE IF EXISTS t_txn_immediate;

-- A long old_parts_lifetime, so that a kept part cannot age out while the test runs.
CREATE TABLE t_txn_default (k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS old_parts_lifetime = 3600;
CREATE TABLE t_txn_immediate (k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS old_parts_lifetime = 3600;

SYSTEM STOP MERGES t_txn_default;
SYSTEM STOP MERGES t_txn_immediate;

INSERT INTO t_txn_default VALUES (1), (2), (3);
INSERT INTO t_txn_immediate VALUES (1), (2), (3);

BEGIN TRANSACTION;
TRUNCATE TABLE t_txn_default;
COMMIT;

BEGIN TRANSACTION;
TRUNCATE TABLE t_txn_immediate SETTINGS truncate_removes_parts_immediately = 1;
COMMIT;

SELECT count() FROM t_txn_default;
SELECT count() FROM t_txn_immediate;

SELECT active, rows, remove_time > toDateTime(0) FROM system.parts WHERE database = currentDatabase() AND table = 't_txn_default' AND rows > 0;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_txn_immediate' AND rows > 0 AND remove_time > toDateTime(0);

DROP TABLE t_txn_default;
DROP TABLE t_txn_immediate;
