-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- Exactly controlled registered replicas are required for `insert_quorum = 'auto'`.

CREATE TABLE auto_quorum_r1 (p UInt8, id UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/auto_quorum_finalize', 'r1')
PARTITION BY p ORDER BY id;

SET async_insert = 0, insert_quorum = 'auto', insert_quorum_timeout = 10000;

-- Repeated fresh single-replica inserts must finalize in both modes.
SET insert_quorum_parallel = 1;
INSERT INTO auto_quorum_r1 VALUES (0, 1);
INSERT INTO auto_quorum_r1 VALUES (0, 2);
SELECT count() FROM system.zookeeper
WHERE path = '/clickhouse/tables/' || currentDatabase() || '/auto_quorum_finalize/quorum/parallel';
SET insert_quorum_parallel = 0;
INSERT INTO auto_quorum_r1 VALUES (0, 3);
INSERT INTO auto_quorum_r1 VALUES (0, 4);
SELECT count() FROM system.zookeeper
WHERE path = '/clickhouse/tables/' || currentDatabase() || '/auto_quorum_finalize/quorum' AND name = 'status';
SELECT id FROM auto_quorum_r1 ORDER BY id SETTINGS select_sequential_consistency = 1;

CREATE TABLE auto_quorum_r2 AS auto_quorum_r1
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/auto_quorum_finalize', 'r2')
PARTITION BY p ORDER BY id;
SYSTEM SYNC REPLICA auto_quorum_r2;

-- An inactive but registered replica still counts toward the majority.
DETACH TABLE auto_quorum_r2 SYNC;
SET insert_quorum_parallel = 0;
INSERT INTO auto_quorum_r1 VALUES (0, 5); -- { serverError TOO_FEW_LIVE_REPLICAS }
SET insert_quorum_parallel = 1;
INSERT INTO auto_quorum_r1 VALUES (0, 5); -- { serverError TOO_FEW_LIVE_REPLICAS }
ATTACH TABLE auto_quorum_r2;
SYSTEM SYNC REPLICA auto_quorum_r2;

-- Preserve each partition's watermark when shrinking from two replicas to one.
SYSTEM STOP MERGES auto_quorum_r1;
SYSTEM STOP MERGES auto_quorum_r2;
SET insert_quorum_parallel = 0;
INSERT INTO auto_quorum_r1 VALUES (0, 5);
INSERT INTO auto_quorum_r1 VALUES (1, 6);
DROP TABLE auto_quorum_r2 SYNC;
INSERT INTO auto_quorum_r1 VALUES (0, 7);
INSERT INTO auto_quorum_r1 VALUES (1, 8);
-- A deduplicated retry must also finish without leaving a pending quorum.
INSERT INTO auto_quorum_r1 VALUES (1, 8);
SELECT id FROM auto_quorum_r1 ORDER BY id SETTINGS select_sequential_consistency = 1;
SELECT count() FROM auto_quorum_r1 SETTINGS select_sequential_consistency = 1, optimize_trivial_count_query = 1;
SELECT count() FROM auto_quorum_r1 SETTINGS select_sequential_consistency = 1, optimize_trivial_count_query = 0;
SELECT count() FROM system.zookeeper
WHERE path = '/clickhouse/tables/' || currentDatabase() || '/auto_quorum_finalize/quorum' AND name = 'status';
SELECT count() FROM system.zookeeper
WHERE path = '/clickhouse/tables/' || currentDatabase() || '/auto_quorum_finalize/quorum/parallel';

DROP TABLE auto_quorum_r1 SYNC;
