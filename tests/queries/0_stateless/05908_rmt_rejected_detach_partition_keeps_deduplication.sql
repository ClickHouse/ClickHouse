-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database: two replicas of one table in one database
-- no-shared-merge-tree: tests the Keeper deduplication blocks of ReplicatedMergeTree

DROP TABLE IF EXISTS t_detach_dedup_r1 SYNC;
DROP TABLE IF EXISTS t_detach_dedup_r2 SYNC;

SET insert_deduplicate = 1, async_insert = 0, insert_keeper_fault_injection_probability = 0;
SET enable_lightweight_update = 1, apply_patch_parts = 1;

CREATE TABLE t_detach_dedup_r1 (p Int64, id Int64, v Int64)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_detach_dedup', 'r1') PARTITION BY p ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
CREATE TABLE t_detach_dedup_r2 (p Int64, id Int64, v Int64)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_detach_dedup', 'r2') PARTITION BY p ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

-- Keep the patch part unapplied.
SYSTEM STOP MERGES t_detach_dedup_r1;
SYSTEM STOP MERGES t_detach_dedup_r2;

INSERT INTO t_detach_dedup_r1 VALUES (1, 1, 100);
INSERT INTO t_detach_dedup_r1 VALUES (2, 2, 100);
UPDATE t_detach_dedup_r1 SET v = 200 WHERE p = 1;

ALTER TABLE t_detach_dedup_r1 DETACH PARTITION 1; -- { serverError SUPPORT_IS_DISABLED }

-- Retries of the same inserts must still be deduplicated.
INSERT INTO t_detach_dedup_r1 VALUES (1, 1, 100);
INSERT INTO t_detach_dedup_r2 VALUES (1, 1, 100);
INSERT INTO t_detach_dedup_r1 VALUES (2, 2, 100);
INSERT INTO t_detach_dedup_r2 VALUES (2, 2, 100);

SYSTEM SYNC REPLICA t_detach_dedup_r1;
SELECT p, id, v FROM t_detach_dedup_r1 ORDER BY ALL;

-- DETACH PARTITION ALL must keep the deduplication blocks of the partitions processed before the rejected one.
DROP TABLE IF EXISTS t_detach_dedup_all SYNC;
CREATE TABLE t_detach_dedup_all (p Int64, id Int64, v Int64)
ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_detach_dedup_all', 'r1') PARTITION BY p ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES t_detach_dedup_all;

INSERT INTO t_detach_dedup_all VALUES (1, 1, 100);
INSERT INTO t_detach_dedup_all VALUES (2, 2, 100);
INSERT INTO t_detach_dedup_all VALUES (3, 3, 100);
INSERT INTO t_detach_dedup_all VALUES (4, 4, 100);
INSERT INTO t_detach_dedup_all VALUES (5, 5, 100);
INSERT INTO t_detach_dedup_all VALUES (6, 6, 100);
INSERT INTO t_detach_dedup_all VALUES (7, 7, 100);
INSERT INTO t_detach_dedup_all VALUES (8, 8, 100);
UPDATE t_detach_dedup_all SET v = 200 WHERE p = 1;

ALTER TABLE t_detach_dedup_all DETACH PARTITION ALL; -- { serverError SUPPORT_IS_DISABLED }

INSERT INTO t_detach_dedup_all VALUES (1, 1, 100);
INSERT INTO t_detach_dedup_all VALUES (2, 2, 100);
INSERT INTO t_detach_dedup_all VALUES (3, 3, 100);
INSERT INTO t_detach_dedup_all VALUES (4, 4, 100);
INSERT INTO t_detach_dedup_all VALUES (5, 5, 100);
INSERT INTO t_detach_dedup_all VALUES (6, 6, 100);
INSERT INTO t_detach_dedup_all VALUES (7, 7, 100);
INSERT INTO t_detach_dedup_all VALUES (8, 8, 100);

SELECT p, id, v FROM t_detach_dedup_all ORDER BY ALL;
DROP TABLE t_detach_dedup_all SYNC;

DROP TABLE t_detach_dedup_r1 SYNC;
DROP TABLE t_detach_dedup_r2 SYNC;
