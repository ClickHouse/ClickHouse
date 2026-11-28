-- Tags: zookeeper

-- An `ALTER TABLE ... CLEAR COLUMN` that runs while a partition or a part is detached does not reach it
-- when it is attached back: the attached part is newer data, as for `MergeTree` and `ALTER UPDATE`.

SET alter_sync = 2, mutations_sync = 2;

DROP TABLE IF EXISTS t_clear_detached_r1 SYNC;
DROP TABLE IF EXISTS t_clear_detached_r2 SYNC;
CREATE TABLE t_clear_detached_r1 (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_detached', 'r1') ORDER BY k;
CREATE TABLE t_clear_detached_r2 (k UInt32, b UInt32, a UInt32 MATERIALIZED b + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_detached', 'r2') ORDER BY k;
INSERT INTO t_clear_detached_r1 (k, b) VALUES (1, 41);
SYSTEM SYNC REPLICA t_clear_detached_r2;
ALTER TABLE t_clear_detached_r1 DETACH PARTITION tuple();
ALTER TABLE t_clear_detached_r1 CLEAR COLUMN b;
ALTER TABLE t_clear_detached_r1 ATTACH PARTITION tuple();
SYSTEM SYNC REPLICA t_clear_detached_r2;
SELECT 'attached', k, b, a FROM t_clear_detached_r1;
SELECT 'attached, other replica', k, b, a FROM t_clear_detached_r2;
OPTIMIZE TABLE t_clear_detached_r1 FINAL;
SYSTEM SYNC REPLICA t_clear_detached_r2;
SELECT 'merged', k, b, a FROM t_clear_detached_r1;
SELECT 'merged, other replica', k, b, a FROM t_clear_detached_r2;
DROP TABLE t_clear_detached_r1 SYNC;
DROP TABLE t_clear_detached_r2 SYNC;

DROP TABLE IF EXISTS t_clear_detached_in_partition SYNC;
CREATE TABLE t_clear_detached_in_partition (k UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_detached_in_partition', 'r1') ORDER BY k;
INSERT INTO t_clear_detached_in_partition VALUES (1, 41);
ALTER TABLE t_clear_detached_in_partition DETACH PARTITION tuple();
ALTER TABLE t_clear_detached_in_partition CLEAR COLUMN b IN PARTITION tuple();
ALTER TABLE t_clear_detached_in_partition ATTACH PARTITION tuple();
SELECT 'in partition', k, b FROM t_clear_detached_in_partition;
DROP TABLE t_clear_detached_in_partition SYNC;

DROP TABLE IF EXISTS t_clear_detached_two_partitions SYNC;
CREATE TABLE t_clear_detached_two_partitions (p UInt8, k UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_detached_two_partitions', 'r1') PARTITION BY p ORDER BY k;
INSERT INTO t_clear_detached_two_partitions VALUES (1, 1, 41), (2, 2, 42);
ALTER TABLE t_clear_detached_two_partitions DETACH PARTITION 1;
ALTER TABLE t_clear_detached_two_partitions CLEAR COLUMN b;
ALTER TABLE t_clear_detached_two_partitions ATTACH PARTITION 1;
SELECT 'two partitions', p, k, b FROM t_clear_detached_two_partitions ORDER BY p;
DROP TABLE t_clear_detached_two_partitions SYNC;

DROP TABLE IF EXISTS t_clear_detached_part SYNC;
CREATE TABLE t_clear_detached_part (k UInt32, b UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_detached_part', 'r1') ORDER BY k;
SYSTEM STOP MERGES t_clear_detached_part;
INSERT INTO t_clear_detached_part VALUES (1, 41);
INSERT INTO t_clear_detached_part VALUES (2, 42);
SYSTEM SYNC REPLICA t_clear_detached_part PULL;
ALTER TABLE t_clear_detached_part DETACH PART 'all_1_1_0';
SYSTEM START MERGES t_clear_detached_part;
ALTER TABLE t_clear_detached_part CLEAR COLUMN b;
ALTER TABLE t_clear_detached_part ATTACH PART 'all_1_1_0';
SELECT 'part', k, b FROM t_clear_detached_part ORDER BY k;
OPTIMIZE TABLE t_clear_detached_part FINAL;
SELECT 'part, merged', k, b FROM t_clear_detached_part ORDER BY k;
DROP TABLE t_clear_detached_part SYNC;

DROP TABLE IF EXISTS t_clear_detached_rename SYNC;
CREATE TABLE t_clear_detached_rename (k UInt32, b UInt32, c UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_detached_rename', 'r1') ORDER BY k;
INSERT INTO t_clear_detached_rename VALUES (1, 41, 7);
ALTER TABLE t_clear_detached_rename DETACH PARTITION tuple();
ALTER TABLE t_clear_detached_rename CLEAR COLUMN c, RENAME COLUMN b TO x;
ALTER TABLE t_clear_detached_rename ATTACH PARTITION tuple();
SELECT 'clear and rename', k, x, c FROM t_clear_detached_rename;
DROP TABLE t_clear_detached_rename SYNC;

DROP TABLE IF EXISTS t_clear_detached_mutate SYNC;
CREATE TABLE t_clear_detached_mutate (k UInt32, b UInt32, c UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_detached_mutate', 'r1') ORDER BY k;
INSERT INTO t_clear_detached_mutate VALUES (1, 41, 7);
ALTER TABLE t_clear_detached_mutate DETACH PARTITION tuple();
ALTER TABLE t_clear_detached_mutate CLEAR COLUMN b;
ALTER TABLE t_clear_detached_mutate ATTACH PARTITION tuple();
ALTER TABLE t_clear_detached_mutate UPDATE c = 8 WHERE 1;
SELECT 'mutated', k, b, c, length(splitByChar('_', _part)) FROM t_clear_detached_mutate;
DROP TABLE t_clear_detached_mutate SYNC;

DROP TABLE IF EXISTS t_clear_detached_index SYNC;
CREATE TABLE t_clear_detached_index (k UInt32, b UInt32, INDEX ib b TYPE minmax GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_detached_index', 'r1') ORDER BY k;
INSERT INTO t_clear_detached_index VALUES (1, 41);
ALTER TABLE t_clear_detached_index DETACH PARTITION tuple();
ALTER TABLE t_clear_detached_index CLEAR COLUMN b;
ALTER TABLE t_clear_detached_index ATTACH PARTITION tuple();
SELECT 'skip index', count() FROM t_clear_detached_index WHERE b = 41;
DROP TABLE t_clear_detached_index SYNC;

DROP TABLE IF EXISTS t_clear_detached_modify SYNC;
CREATE TABLE t_clear_detached_modify (k UInt32, b UInt32, INDEX ib b TYPE minmax GRANULARITY 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_detached_modify', 'r1') ORDER BY k;
INSERT INTO t_clear_detached_modify VALUES (1, 41);
ALTER TABLE t_clear_detached_modify DETACH PARTITION tuple();
ALTER TABLE t_clear_detached_modify MODIFY COLUMN b String;
ALTER TABLE t_clear_detached_modify ATTACH PARTITION tuple();
SELECT 'modify', k, b, toTypeName(b), (SELECT count() FROM t_clear_detached_modify WHERE b = '41') FROM t_clear_detached_modify;
DROP TABLE t_clear_detached_modify SYNC;
