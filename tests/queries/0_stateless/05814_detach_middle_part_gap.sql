-- Tags: no-replicated-database, no-shared-merge-tree
DROP TABLE IF EXISTS t_detach_middle_gap;
CREATE TABLE t_detach_middle_gap (k UInt32) ENGINE = MergeTree ORDER BY k
SETTINGS remove_empty_parts = 0, old_parts_lifetime = 3600;
INSERT INTO t_detach_middle_gap SETTINGS async_insert = 0 VALUES (1);
OPTIMIZE TABLE t_detach_middle_gap FINAL SETTINGS optimize_skip_merged_partitions = 0;
SYSTEM STOP MERGES t_detach_middle_gap;
INSERT INTO t_detach_middle_gap SETTINGS async_insert = 0 VALUES (2);
INSERT INTO t_detach_middle_gap SETTINGS async_insert = 0 VALUES (3);
ALTER TABLE t_detach_middle_gap DETACH PART 'all_2_2_0';
SYSTEM START MERGES t_detach_middle_gap;
OPTIMIZE TABLE t_detach_middle_gap FINAL SETTINGS optimize_throw_if_noop = 0;
ALTER TABLE t_detach_middle_gap DETACH PARTITION tuple();
ALTER TABLE t_detach_middle_gap ATTACH PARTITION tuple();
SELECT count(), sum(k) FROM t_detach_middle_gap;
OPTIMIZE TABLE t_detach_middle_gap FINAL;
SELECT count(), sum(k) FROM t_detach_middle_gap;
DROP TABLE t_detach_middle_gap;

CREATE TABLE t_detach_middle_gap (k UInt32) ENGINE = MergeTree ORDER BY k
SETTINGS remove_empty_parts = 0, old_parts_lifetime = 0;
INSERT INTO t_detach_middle_gap SETTINGS async_insert = 0 VALUES (1);
OPTIMIZE TABLE t_detach_middle_gap FINAL SETTINGS optimize_skip_merged_partitions = 0;
SYSTEM STOP MERGES t_detach_middle_gap;
INSERT INTO t_detach_middle_gap SETTINGS async_insert = 0 VALUES (2);
INSERT INTO t_detach_middle_gap SETTINGS async_insert = 0 VALUES (3);
ALTER TABLE t_detach_middle_gap DETACH PART 'all_2_2_0';
DETACH TABLE t_detach_middle_gap;
ATTACH TABLE t_detach_middle_gap;
OPTIMIZE TABLE t_detach_middle_gap FINAL SETTINGS optimize_throw_if_noop = 0;
SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_detach_middle_gap' AND active;
ALTER TABLE t_detach_middle_gap DROP DETACHED PART 'all_2_2_0' SETTINGS allow_drop_detached = 1;
OPTIMIZE TABLE t_detach_middle_gap FINAL;
SELECT count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_detach_middle_gap' AND active;
SELECT count(), sum(k) FROM t_detach_middle_gap;
DROP TABLE t_detach_middle_gap;
