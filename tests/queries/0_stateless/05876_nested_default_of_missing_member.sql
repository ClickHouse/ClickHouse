-- A DEFAULT or MATERIALIZED Nested member computed from another member that a part does not store:
-- the missing member must be read as arrays of the sizes shared with the stored members (#134561).

DROP TABLE IF EXISTS t_wide;
DROP TABLE IF EXISTS t_compact;
DROP TABLE IF EXISTS t_vertical;
DROP TABLE IF EXISTS t_default;

SELECT 'wide';
CREATE TABLE t_wide (id UInt64, n Nested(a String, c String)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_vertical_merge_algorithm = 0;
INSERT INTO t_wide VALUES (1, ['a', 'b'], ['x', 'y']);
ALTER TABLE t_wide ADD COLUMN `n.d` Array(String);
ALTER TABLE t_wide ADD COLUMN `n.e` Array(String) MATERIALIZED arrayMap(x -> concat('e', x), `n.d`);
SELECT `n.e` FROM t_wide;
SELECT `n.a`, `n.d`, `n.e` FROM t_wide;
SELECT count() FROM t_wide WHERE has(`n.e`, 'e');
SELECT x FROM t_wide ARRAY JOIN `n.e` AS x;
OPTIMIZE TABLE t_wide FINAL;
SELECT `n.e` FROM t_wide;
SELECT `n.a`, `n.c`, `n.d`, `n.e` FROM t_wide;

SELECT 'compact';
CREATE TABLE t_compact (id UInt64, n Nested(a String, c String)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, enable_vertical_merge_algorithm = 0;
INSERT INTO t_compact VALUES (1, ['a', 'b'], ['x', 'y']);
ALTER TABLE t_compact ADD COLUMN `n.d` Array(String);
ALTER TABLE t_compact ADD COLUMN `n.e` Array(String) MATERIALIZED arrayMap(x -> concat('e', x), `n.d`);
SELECT `n.e` FROM t_compact;
OPTIMIZE TABLE t_compact FINAL;
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_compact' AND active;
SELECT `n.a`, `n.c`, `n.d`, `n.e` FROM t_compact;

SELECT 'vertical';
CREATE TABLE t_vertical (id UInt64, n Nested(a String, c String)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_bytes_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1, min_bytes_for_full_part_storage = 0;
INSERT INTO t_vertical VALUES (1, ['a', 'b'], ['x', 'y']);
ALTER TABLE t_vertical ADD COLUMN `n.d` Array(String);
ALTER TABLE t_vertical ADD COLUMN `n.e` Array(String) MATERIALIZED arrayMap(x -> concat('e', x), `n.d`);
INSERT INTO t_vertical (id, `n.a`, `n.c`, `n.d`) VALUES (2, ['p'], ['q'], ['r']);
OPTIMIZE TABLE t_vertical FINAL;
SELECT id, `n.a`, `n.c`, `n.d`, `n.e` FROM t_vertical ORDER BY id;
SYSTEM FLUSH LOGS part_log;
SELECT merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND table = 't_vertical' AND event_type = 'MergeParts';

SELECT 'default';
CREATE TABLE t_default (id UInt64, n Nested(a String)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_vertical_merge_algorithm = 0;
INSERT INTO t_default VALUES (1, ['a', 'b']);
ALTER TABLE t_default ADD COLUMN `n.u` Array(Array(String));
ALTER TABLE t_default ADD COLUMN `n.v` Array(Array(String)) DEFAULT arrayMap(x -> arrayMap(y -> concat('v', y), x), `n.u`);
SELECT `n.v` FROM t_default;
OPTIMIZE TABLE t_default FINAL;
SELECT `n.a`, `n.u`, `n.v` FROM t_default;

DROP TABLE t_wide;
DROP TABLE t_compact;
DROP TABLE t_vertical;
DROP TABLE t_default;
