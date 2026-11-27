-- Tags: no-replicated-database, no-shared-merge-tree
-- Tag no-replicated-database, no-shared-merge-tree: replicated and shared tables do not keep the truncated parts

DROP TABLE IF EXISTS t_default;
DROP TABLE IF EXISTS t_immediate;
DROP TABLE IF EXISTS t_compat;

-- A long old_parts_lifetime, so that a kept part cannot age out while the test runs.
CREATE TABLE t_default (k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS old_parts_lifetime = 3600;
CREATE TABLE t_immediate (k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS old_parts_lifetime = 3600;
CREATE TABLE t_compat (k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS old_parts_lifetime = 3600;

SYSTEM STOP MERGES t_default;
SYSTEM STOP MERGES t_immediate;
SYSTEM STOP MERGES t_compat;

INSERT INTO t_default VALUES (1), (2), (3);
INSERT INTO t_immediate VALUES (1), (2), (3);
INSERT INTO t_compat VALUES (1), (2), (3);

TRUNCATE TABLE t_default;
TRUNCATE TABLE t_immediate SETTINGS truncate_removes_parts_immediately = 1;
TRUNCATE TABLE t_compat SETTINGS compatibility = '26.10';

SELECT count() FROM t_default;
SELECT count() FROM t_immediate;
SELECT count() FROM t_compat;

SELECT active, rows, remove_time > toDateTime(0) FROM system.parts WHERE database = currentDatabase() AND table = 't_default' AND rows > 0;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_immediate' AND rows > 0 AND remove_time > toDateTime(0);
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_compat' AND rows > 0 AND remove_time > toDateTime(0);

DETACH TABLE t_default;
ATTACH TABLE t_default;
SELECT count() FROM t_default;

DROP TABLE t_default;
DROP TABLE t_immediate;
DROP TABLE t_compat;
