-- A physical column `s.x` in the key is independent of a column `s`: renaming `s` must not rewrite the key,
-- whether the key refers to it as the compound identifier `s.x` or as the quoted name `s.x`.

DROP TABLE IF EXISTS t_compound;
DROP TABLE IF EXISTS t_quoted;
DROP TABLE IF EXISTS t_subcolumn;

CREATE TABLE t_compound (s UInt32, `s.x` UInt32) ENGINE = MergeTree ORDER BY s.x SETTINGS share_nested_offsets = 0;
INSERT INTO t_compound VALUES (1, 20), (2, 10);
ALTER TABLE t_compound RENAME COLUMN s TO renamed_s;
SELECT renamed_s, `s.x` FROM t_compound ORDER BY `s.x`;
SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_compound';
DETACH TABLE t_compound;
ATTACH TABLE t_compound;
SELECT renamed_s, `s.x` FROM t_compound ORDER BY `s.x`;

CREATE TABLE t_quoted (s UInt32, `s.x` UInt32) ENGINE = MergeTree ORDER BY `s.x` SETTINGS share_nested_offsets = 0;
INSERT INTO t_quoted VALUES (1, 20), (2, 10);
ALTER TABLE t_quoted RENAME COLUMN s TO renamed_s;
SELECT renamed_s, `s.x` FROM t_quoted ORDER BY `s.x`;
SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_quoted';
DETACH TABLE t_quoted;
ATTACH TABLE t_quoted;
SELECT renamed_s, `s.x` FROM t_quoted ORDER BY `s.x`;

-- The key refers to a subcolumn of the independent column `s.x`.
CREATE TABLE t_subcolumn (s UInt32, `s.x` Tuple(y UInt32)) ENGINE = MergeTree PARTITION BY s.x.y ORDER BY s.x.y SETTINGS share_nested_offsets = 0;
INSERT INTO t_subcolumn VALUES (1, tuple(20)), (2, tuple(10));
ALTER TABLE t_subcolumn RENAME COLUMN s TO renamed_s;
SELECT renamed_s, `s.x`.y FROM t_subcolumn ORDER BY `s.x`.y;
SELECT partition_key, sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_subcolumn';
DETACH TABLE t_subcolumn;
ATTACH TABLE t_subcolumn;
SELECT renamed_s, `s.x`.y FROM t_subcolumn ORDER BY `s.x`.y;

DROP TABLE t_compound;
DROP TABLE t_quoted;
DROP TABLE t_subcolumn;
