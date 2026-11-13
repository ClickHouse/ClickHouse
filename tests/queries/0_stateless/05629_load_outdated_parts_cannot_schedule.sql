-- Tags: no-shared-merge-tree, no-parallel, no-fasttest
-- no-shared-merge-tree: SharedMergeTree doesn't load inactive parts to memory after restart
-- no-parallel: SYSTEM ENABLE FAILPOINT is process-wide

DROP TABLE IF EXISTS t_load_outdated_parts_cannot_schedule;

CREATE TABLE t_load_outdated_parts_cannot_schedule (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS old_parts_lifetime = 600;

INSERT INTO t_load_outdated_parts_cannot_schedule VALUES (1);
INSERT INTO t_load_outdated_parts_cannot_schedule VALUES (2);
INSERT INTO t_load_outdated_parts_cannot_schedule VALUES (3);
OPTIMIZE TABLE t_load_outdated_parts_cannot_schedule FINAL;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_load_outdated_parts_cannot_schedule' AND NOT active;

DETACH TABLE t_load_outdated_parts_cannot_schedule;

SYSTEM ENABLE FAILPOINT merge_tree_load_outdated_parts_cannot_schedule;
ATTACH TABLE t_load_outdated_parts_cannot_schedule;

SELECT count() FROM t_load_outdated_parts_cannot_schedule;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_load_outdated_parts_cannot_schedule' AND NOT active;

SYSTEM DISABLE FAILPOINT merge_tree_load_outdated_parts_cannot_schedule;
SYSTEM WAIT LOADING PARTS t_load_outdated_parts_cannot_schedule;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_load_outdated_parts_cannot_schedule' AND NOT active;

DROP TABLE t_load_outdated_parts_cannot_schedule;
