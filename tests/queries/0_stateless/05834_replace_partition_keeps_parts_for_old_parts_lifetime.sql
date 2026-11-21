-- Tags: no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database, no-shared-merge-tree: replicated and shared tables remove the replaced parts at once

DROP TABLE IF EXISTS t_dst;
DROP TABLE IF EXISTS t_dst_merged;
DROP TABLE IF EXISTS t_src;

-- A long old_parts_lifetime, so that a kept part cannot age out while the test runs.
CREATE TABLE t_dst (k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS old_parts_lifetime = 3600;
CREATE TABLE t_dst_merged (k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS old_parts_lifetime = 3600;
CREATE TABLE t_src (k UInt32) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_src VALUES (4), (5);

-- The replaced part stays on disk as an inactive part, and is not resurrected by a reload.
SYSTEM STOP MERGES t_dst;
INSERT INTO t_dst VALUES (1), (2), (3);
ALTER TABLE t_dst REPLACE PARTITION tuple() FROM t_src;
SELECT k FROM t_dst ORDER BY k;
SELECT active, remove_time > toDateTime(0) FROM system.parts WHERE database = currentDatabase() AND table = 't_dst' AND rows = 3;
DETACH TABLE t_dst;
ATTACH TABLE t_dst;
SELECT k FROM t_dst ORDER BY k;

-- The parts outdated by an earlier merge keep their own removal time.
INSERT INTO t_dst_merged VALUES (1);
INSERT INTO t_dst_merged VALUES (2);
OPTIMIZE TABLE t_dst_merged FINAL;
SYSTEM STOP MERGES t_dst_merged;
ALTER TABLE t_dst_merged REPLACE PARTITION tuple() FROM t_src;
SELECT k FROM t_dst_merged ORDER BY k;
SELECT count() >= 3, countIf(remove_time = toDateTime(0)) FROM system.parts WHERE database = currentDatabase() AND table = 't_dst_merged' AND NOT active AND rows > 0;

DROP TABLE t_dst;
DROP TABLE t_dst_merged;
DROP TABLE t_src;
