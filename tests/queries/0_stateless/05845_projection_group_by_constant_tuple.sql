-- Random settings limits: optimize_use_projections=(1, 1)
-- Projections that group by a tuple of constants.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (x Int32, PROJECTION p (SELECT tuple(1, 2) GROUP BY tuple(1, 2))) ENGINE = MergeTree ORDER BY x;
INSERT INTO tab VALUES (1), (2);
INSERT INTO tab VALUES (3);
SELECT x, count() FROM tab GROUP BY x ORDER BY x;
SELECT tuple(1, 2) FROM tab GROUP BY tuple(1, 2) SETTINGS force_optimize_projection_name = 'p';
OPTIMIZE TABLE tab FINAL;
SELECT rows FROM system.projection_parts WHERE database = currentDatabase() AND table = 'tab' AND active;
SELECT column, type FROM system.projection_parts_columns WHERE database = currentDatabase() AND table = 'tab' AND active;
DROP TABLE tab;

CREATE TABLE tab (x Int32, PROJECTION p (SELECT ((1::UInt8), (2::Int32)) WHERE x > 0 GROUP BY ((1::UInt8), (2::Int32)))) ENGINE = MergeTree ORDER BY x;
INSERT INTO tab VALUES (1), (2);
SELECT x, count() FROM tab GROUP BY x ORDER BY x;
DROP TABLE tab;

CREATE TABLE tab (x Int32, PROJECTION p (SELECT grouping(tuple(1, 2)) GROUP BY tuple(1, 2))) ENGINE = MergeTree ORDER BY x;
SELECT name FROM system.projections WHERE database = currentDatabase() AND table = 'tab';
INSERT INTO tab VALUES (1);
SELECT count() FROM tab;
DROP TABLE tab;

CREATE TABLE tab (x Int32) ENGINE = MergeTree ORDER BY x;
INSERT INTO tab VALUES (1);
ALTER TABLE tab ADD PROJECTION p (SELECT tuple(1) AS k GROUP BY k);
SELECT x, count() FROM tab GROUP BY x;
ALTER TABLE tab MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;
SELECT rows FROM system.projection_parts WHERE database = currentDatabase() AND table = 'tab' AND active;
DROP TABLE tab;

CREATE TABLE tab (x Int32, PROJECTION p (SELECT tuple(3, 4) GROUP BY tuple(1, 2), tuple(3, 4))) ENGINE = MergeTree ORDER BY x;
INSERT INTO tab VALUES (1);
SELECT count() FROM tab;
DROP TABLE tab;

-- A tuple that the query writes stays one key.
SELECT grouping(tuple(1, 2)) GROUP BY tuple(1, 2);
