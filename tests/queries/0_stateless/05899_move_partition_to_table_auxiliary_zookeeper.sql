-- Tags: zookeeper, no-fasttest, no-shared-merge-tree
-- no-shared-merge-tree: SharedMergeTree does not support auxiliary ZooKeepers

-- zookeeper2 is an auxiliary ZooKeeper with its own chroot, so the same path on it is a different node.

DROP TABLE IF EXISTS src SYNC;
DROP TABLE IF EXISTS dst_r1 SYNC;
DROP TABLE IF EXISTS dst_r2 SYNC;
DROP TABLE IF EXISTS mig_src SYNC;
DROP TABLE IF EXISTS mig_dst_r1 SYNC;
DROP TABLE IF EXISTS mig_dst_r2 SYNC;

CREATE TABLE src (p UInt8, x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/src', 'r1') PARTITION BY p ORDER BY x;
CREATE TABLE dst_r1 (p UInt8, x UInt32) ENGINE = ReplicatedMergeTree('zookeeper2:/clickhouse/tables/{database}/dst', 'r1') PARTITION BY p ORDER BY x;
CREATE TABLE dst_r2 (p UInt8, x UInt32) ENGINE = ReplicatedMergeTree('zookeeper2:/clickhouse/tables/{database}/dst', 'r2') PARTITION BY p ORDER BY x;

INSERT INTO src VALUES (1, 1), (1, 2), (2, 3);

ALTER TABLE src MOVE PARTITION 1 TO TABLE dst_r1;
SYSTEM SYNC REPLICA dst_r2;
SELECT 'default to zookeeper2', (SELECT count() FROM src), (SELECT count() FROM dst_r1), (SELECT count() FROM dst_r2);

ALTER TABLE dst_r2 MOVE PARTITION 1 TO TABLE src;
SYSTEM SYNC REPLICA dst_r1;
SELECT 'zookeeper2 to default', (SELECT count() FROM src), (SELECT count() FROM dst_r1), (SELECT count() FROM dst_r2);

-- The same path and replica name on both ZooKeepers, as when a table is migrated to another Keeper.
CREATE TABLE mig_src (p UInt8, x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/mig', 'r1') PARTITION BY p ORDER BY x;
CREATE TABLE mig_dst_r1 (p UInt8, x UInt32) ENGINE = ReplicatedMergeTree('zookeeper2:/clickhouse/tables/{database}/mig', 'r1') PARTITION BY p ORDER BY x;
CREATE TABLE mig_dst_r2 (p UInt8, x UInt32) ENGINE = ReplicatedMergeTree('zookeeper2:/clickhouse/tables/{database}/mig', 'r2') PARTITION BY p ORDER BY x;

INSERT INTO mig_src VALUES (1, 1);
INSERT INTO mig_dst_r1 VALUES (2, 2);

ALTER TABLE mig_src MOVE PARTITION 1 TO TABLE mig_dst_r1;
SYSTEM SYNC REPLICA mig_dst_r2;
SELECT 'same path', (SELECT count() FROM mig_src), (SELECT count() FROM mig_dst_r1), (SELECT count() FROM mig_dst_r2);

DROP TABLE src SYNC;
DROP TABLE dst_r1 SYNC;
DROP TABLE dst_r2 SYNC;
DROP TABLE mig_src SYNC;
DROP TABLE mig_dst_r1 SYNC;
DROP TABLE mig_dst_r2 SYNC;
