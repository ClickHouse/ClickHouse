-- The partition min/max index of a part written with `optimize_on_insert` must reflect only the rows left after the insert-time merge.
-- https://github.com/ClickHouse/ClickHouse/issues/125273

SET optimize_on_insert = 1;

DROP TABLE IF EXISTS t_replacing;
CREATE TABLE t_replacing (k String, v Int8) ENGINE = ReplacingMergeTree ORDER BY k PARTITION BY v - v;
INSERT INTO t_replacing VALUES ('', 100), ('', 1);

SELECT min(v), max(v), count() FROM t_replacing;
SELECT min(v), max(v), count() FROM t_replacing SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM t_replacing WHERE v = 100;
DROP TABLE t_replacing;

-- The shape found by SQLancer: rows with equal sorting keys and versions, the last one survives.
DROP TABLE IF EXISTS t_versioned;
CREATE TABLE t_versioned (c0 String, c1 DateTime, c2 Int8) ENGINE = ReplacingMergeTree(c1) ORDER BY c0 PARTITION BY c2 - c2;
INSERT INTO t_versioned (c2) VALUES (113), (-91), (-90);
INSERT INTO t_versioned (c2) VALUES (84);

SELECT max(c2) = finalizeAggregation(arrayReduce('maxState', groupArray(c2))) FROM t_versioned;
SELECT min(c2), max(c2) FROM t_versioned;
DROP TABLE t_versioned;

-- The old format restores the min/max index of the date column from the part name when the part is loaded.
DROP TABLE IF EXISTS t_legacy;
SET allow_deprecated_syntax_for_merge_tree = 1;
CREATE TABLE t_legacy (d Date, id UInt64, ver UInt64) ENGINE = ReplacingMergeTree(d, id, 8192, ver);
INSERT INTO t_legacy VALUES ('2024-01-05', 1, 1), ('2024-01-20', 1, 2);
SELECT min(d), max(d) FROM t_legacy;
ALTER TABLE t_legacy DETACH PARTITION 202401;
ALTER TABLE t_legacy ATTACH PARTITION 202401;
SELECT min(d), max(d) FROM t_legacy;
SELECT min(d), max(d) FROM t_legacy SETTINGS optimize_use_implicit_projections = 0;
SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_legacy' AND active;
DROP TABLE t_legacy;
