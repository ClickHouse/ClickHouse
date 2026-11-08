-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database: the part names below are those of a non-replicated MergeTree.
-- no-shared-merge-tree: the test is about the empty parts that DETACH of a non-replicated MergeTree commits.

-- DETACH covers the detached parts with empty parts. An empty part that stays active (younger than
-- `empty_parts_lifetime`, or kept by `remove_empty_parts = 0`) was copied to `detached/` by a later DETACH,
-- directly or through a part merged from it, and ATTACH then renamed the earlier detached part to `inactive_`.

DROP TABLE IF EXISTS t_detach_lifetime;
DROP TABLE IF EXISTS t_detach_keep_empty;
DROP TABLE IF EXISTS t_detach_part;

SELECT 'empty_parts_lifetime';
CREATE TABLE t_detach_lifetime (p UInt8, k UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS empty_parts_lifetime = 86400;
INSERT INTO t_detach_lifetime SETTINGS async_insert = 0 VALUES (1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3);
-- Partition 1 is detached twice.
ALTER TABLE t_detach_lifetime DETACH PARTITION 1;
ALTER TABLE t_detach_lifetime DETACH PARTITION 1;
-- Partition 2 gets a new row and a merge before it is detached again.
ALTER TABLE t_detach_lifetime DETACH PARTITION 2;
INSERT INTO t_detach_lifetime SETTINGS async_insert = 0 VALUES (2, 4);
OPTIMIZE TABLE t_detach_lifetime PARTITION 2 FINAL;
ALTER TABLE t_detach_lifetime DETACH PARTITION 2;
ALTER TABLE t_detach_lifetime ATTACH PARTITION ALL;
SELECT p, count() FROM t_detach_lifetime GROUP BY p ORDER BY p;
SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_detach_lifetime';

SELECT 'remove_empty_parts = 0';
CREATE TABLE t_detach_keep_empty (p UInt8, k UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS remove_empty_parts = 0;
INSERT INTO t_detach_keep_empty SETTINGS async_insert = 0 VALUES (1, 1), (1, 2), (1, 3), (2, 1), (2, 2), (2, 3);
ALTER TABLE t_detach_keep_empty DETACH PARTITION 1;
ALTER TABLE t_detach_keep_empty DETACH PARTITION 1;
ALTER TABLE t_detach_keep_empty DETACH PARTITION 2;
INSERT INTO t_detach_keep_empty SETTINGS async_insert = 0 VALUES (2, 4);
OPTIMIZE TABLE t_detach_keep_empty PARTITION 2 FINAL;
ALTER TABLE t_detach_keep_empty DETACH PARTITION 2;
ALTER TABLE t_detach_keep_empty ATTACH PARTITION ALL;
SELECT p, count() FROM t_detach_keep_empty GROUP BY p ORDER BY p;
SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_detach_keep_empty';

SELECT 'DETACH PART';
CREATE TABLE t_detach_part (k UInt32) ENGINE = MergeTree ORDER BY k
SETTINGS remove_empty_parts = 0;
INSERT INTO t_detach_part SETTINGS async_insert = 0 VALUES (1);
ALTER TABLE t_detach_part DETACH PART 'all_1_1_0';
INSERT INTO t_detach_part SETTINGS async_insert = 0 VALUES (2);
OPTIMIZE TABLE t_detach_part FINAL;
ALTER TABLE t_detach_part DETACH PARTITION tuple();
ALTER TABLE t_detach_part ATTACH PARTITION tuple();
SELECT count(), sum(k) FROM t_detach_part;
SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_detach_part';

DROP TABLE t_detach_lifetime;
DROP TABLE t_detach_keep_empty;
DROP TABLE t_detach_part;
