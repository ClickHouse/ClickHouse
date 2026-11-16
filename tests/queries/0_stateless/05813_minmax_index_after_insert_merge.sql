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
