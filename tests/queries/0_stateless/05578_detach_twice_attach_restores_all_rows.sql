-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database: the part names below are those of a non-replicated MergeTree.
-- no-shared-merge-tree: the test is about the empty parts that DETACH of a non-replicated MergeTree commits.

-- DETACH covers the detached parts with empty parts. An empty part that stays active (younger than
-- `empty_parts_lifetime`, or kept by `remove_empty_parts = 0`) was copied to `detached/` by a later DETACH,
-- directly or through a part merged from it, and ATTACH then renamed the earlier detached part to `inactive_`.
-- An empty part that is only outdated in memory is loaded as active again after a reload of the table,
-- so it is outdated again when it covers a detached part.

DROP TABLE IF EXISTS t_detach_lifetime;
DROP TABLE IF EXISTS t_detach_keep_empty;
DROP TABLE IF EXISTS t_detach_part;
DROP TABLE IF EXISTS t_detach_reload;
DROP TABLE IF EXISTS t_detach_readonly;
DROP TABLE IF EXISTS t_detach_loading;

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

SELECT 'DETACH TABLE';
-- `old_parts_lifetime` keeps the empty part of the DETACH on disk until the table is loaded again.
CREATE TABLE t_detach_reload (p UInt8, k UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS remove_empty_parts = 0, old_parts_lifetime = 3600;
INSERT INTO t_detach_reload SETTINGS async_insert = 0 VALUES (1, 1), (1, 2), (1, 3);
ALTER TABLE t_detach_reload DETACH PARTITION 1;
DETACH TABLE t_detach_reload;
ATTACH TABLE t_detach_reload;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_detach_reload' AND active;
-- A merge of the new row with an active empty part would cover the detached part as well.
INSERT INTO t_detach_reload SETTINGS async_insert = 0 VALUES (1, 4);
OPTIMIZE TABLE t_detach_reload PARTITION 1 FINAL;
ALTER TABLE t_detach_reload DETACH PARTITION 1;
ALTER TABLE t_detach_reload ATTACH PARTITION 1;
SELECT count(), sum(k) FROM t_detach_reload;
SELECT count() FROM system.detached_parts WHERE database = currentDatabase() AND table = 't_detach_reload';

SELECT 'table_readonly';
-- A table loaded read-only outdates the empty part when it becomes writable.
CREATE TABLE t_detach_readonly (p UInt8, k UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS remove_empty_parts = 0, old_parts_lifetime = 3600;
INSERT INTO t_detach_readonly SETTINGS async_insert = 0 VALUES (1, 1), (1, 2), (1, 3);
ALTER TABLE t_detach_readonly DETACH PARTITION 1;
ALTER TABLE t_detach_readonly MODIFY SETTING table_readonly = 1;
DETACH TABLE t_detach_readonly;
ATTACH TABLE t_detach_readonly;
ALTER TABLE t_detach_readonly MODIFY SETTING table_readonly = 0;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_detach_readonly' AND active;
INSERT INTO t_detach_readonly SETTINGS async_insert = 0 VALUES (1, 4);
OPTIMIZE TABLE t_detach_readonly PARTITION 1 FINAL;
ALTER TABLE t_detach_readonly DETACH PARTITION 1;
ALTER TABLE t_detach_readonly ATTACH PARTITION 1;
SELECT count(), sum(k) FROM t_detach_readonly;

SELECT 'ATTACH TABLE before the outdated parts are loaded';
-- The empty part outdated on load stays on disk while the outdated parts it covers are not loaded yet,
-- otherwise the cleanup removes it and the next load makes them active again.
CREATE TABLE t_detach_loading (p UInt8, k UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY k
SETTINGS remove_empty_parts = 0, old_parts_lifetime = 3600, sleep_before_loading_outdated_parts_ms = 6000,
    merge_tree_clear_old_parts_interval_seconds = 1, cleanup_delay_period = 1, cleanup_delay_period_random_add = 0, max_cleanup_delay_period = 1;
INSERT INTO t_detach_loading SETTINGS async_insert = 0 VALUES (1, 1);
INSERT INTO t_detach_loading SETTINGS async_insert = 0 VALUES (1, 2), (1, 3);
OPTIMIZE TABLE t_detach_loading PARTITION 1 FINAL;
ALTER TABLE t_detach_loading DETACH PARTITION 1;
-- The outdated parts stay on disk until the table is detached, and are loaded 6 seconds after it is attached.
SYSTEM STOP CLEANUP t_detach_loading;
ALTER TABLE t_detach_loading MODIFY SETTING old_parts_lifetime = 1;
DETACH TABLE t_detach_loading;
ATTACH TABLE t_detach_loading;
SELECT sleep(3) FORMAT Null;
DETACH TABLE t_detach_loading;
ATTACH TABLE t_detach_loading;
SELECT count() FROM t_detach_loading;
ALTER TABLE t_detach_loading ATTACH PARTITION 1;
SELECT count(), sum(k) FROM t_detach_loading;

DROP TABLE t_detach_lifetime;
DROP TABLE t_detach_keep_empty;
DROP TABLE t_detach_part;
DROP TABLE t_detach_reload;
DROP TABLE t_detach_readonly;
DROP TABLE t_detach_loading;
