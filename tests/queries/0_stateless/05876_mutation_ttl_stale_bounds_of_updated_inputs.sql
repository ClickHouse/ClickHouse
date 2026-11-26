-- An UPDATE of a TTL input must evaluate that TTL on the updated rows, not on the bound stored for the old values.
-- Every part below has an expired stored TTL bound; the UPDATE moves one row (or all rows) to the future.
-- Wide parts: `min_bytes_for_wide_part = 0`; Compact parts: wide thresholds out of reach.

DROP TABLE IF EXISTS t_col_wide;
DROP TABLE IF EXISTS t_col_compact;
DROP TABLE IF EXISTS t_col_wide_partial;
DROP TABLE IF EXISTS t_rows_wide;
DROP TABLE IF EXISTS t_rows_compact;
DROP TABLE IF EXISTS t_rows_drop_parts;
DROP TABLE IF EXISTS t_rows_materialized;
DROP TABLE IF EXISTS t_rows_where;

-- Column TTL, Wide part, all columns rewritten.
CREATE TABLE t_col_wide (d DateTime('UTC'), w String TTL d + INTERVAL 1 SECOND)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t_col_wide;
INSERT INTO t_col_wide VALUES ('2020-01-01 00:00:00', 'a'), ('2020-01-01 00:00:00', 'b');
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_col_wide' AND active;
ALTER TABLE t_col_wide UPDATE d = toDateTime('2090-01-01 00:00:00', 'UTC') WHERE w = 'a' SETTINGS mutations_sync = 2;
SELECT 'col_wide', d, w FROM t_col_wide ORDER BY d DESC;
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_col_wide' AND active AND column IN ('d', 'w') ORDER BY column;

-- Column TTL, Compact part.
CREATE TABLE t_col_compact (d DateTime('UTC'), w String TTL d + INTERVAL 1 SECOND)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t_col_compact;
INSERT INTO t_col_compact VALUES ('2020-01-01 00:00:00', 'a'), ('2020-01-01 00:00:00', 'b');
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_col_compact' AND active;
ALTER TABLE t_col_compact UPDATE d = toDateTime('2090-01-01 00:00:00', 'UTC') WHERE w = 'a' SETTINGS mutations_sync = 2;
SELECT 'col_compact', d, w FROM t_col_compact ORDER BY d DESC;

-- Column TTL, Wide part, only some columns rewritten (`v` is untouched).
CREATE TABLE t_col_wide_partial (d DateTime('UTC'), w String TTL d + INTERVAL 1 SECOND, v String)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t_col_wide_partial;
INSERT INTO t_col_wide_partial VALUES ('2020-01-01 00:00:00', 'a', 'x'), ('2020-01-01 00:00:00', 'b', 'y');
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_col_wide_partial' AND active;
ALTER TABLE t_col_wide_partial UPDATE d = toDateTime('2090-01-01 00:00:00', 'UTC') WHERE w = 'a' SETTINGS mutations_sync = 2;
SELECT 'col_wide_partial', d, w, v FROM t_col_wide_partial ORDER BY d DESC;

-- Rows TTL, Wide part.
CREATE TABLE t_rows_wide (k UInt8, d DateTime('UTC'))
ENGINE = MergeTree ORDER BY k TTL d + INTERVAL 1 SECOND
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t_rows_wide;
INSERT INTO t_rows_wide VALUES (1, '2020-01-01 00:00:00'), (2, '2020-01-01 00:00:00');
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_rows_wide' AND active;
ALTER TABLE t_rows_wide UPDATE d = toDateTime('2090-01-01 00:00:00', 'UTC') WHERE k = 1 SETTINGS mutations_sync = 2;
SELECT 'rows_wide', k FROM t_rows_wide ORDER BY k;
SELECT toUnixTimestamp(delete_ttl_info_max) = toUnixTimestamp(toDateTime('2090-01-01 00:00:01', 'UTC'))
FROM system.parts WHERE database = currentDatabase() AND table = 't_rows_wide' AND active;

-- Rows TTL, Compact part.
CREATE TABLE t_rows_compact (k UInt8, d DateTime('UTC'))
ENGINE = MergeTree ORDER BY k TTL d + INTERVAL 1 SECOND
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t_rows_compact;
INSERT INTO t_rows_compact VALUES (1, '2020-01-01 00:00:00'), (2, '2020-01-01 00:00:00');
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_rows_compact' AND active;
ALTER TABLE t_rows_compact UPDATE d = toDateTime('2090-01-01 00:00:00', 'UTC') WHERE k = 1 SETTINGS mutations_sync = 2;
SELECT 'rows_compact', k FROM t_rows_compact ORDER BY k;

-- Rows TTL with `ttl_only_drop_parts`: the part is not dropped once no row is expired.
CREATE TABLE t_rows_drop_parts (k UInt8, d DateTime('UTC'))
ENGINE = MergeTree ORDER BY k TTL d + INTERVAL 1 SECOND
SETTINGS ttl_only_drop_parts = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t_rows_drop_parts;
INSERT INTO t_rows_drop_parts VALUES (1, '2020-01-01 00:00:00'), (2, '2020-01-01 00:00:00');
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_rows_drop_parts' AND active;
ALTER TABLE t_rows_drop_parts UPDATE d = toDateTime('2090-01-01 00:00:00', 'UTC') WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'rows_drop_parts', count() FROM t_rows_drop_parts;

-- Rows TTL on a `MATERIALIZED` column recomputed from the updated one.
CREATE TABLE t_rows_materialized (k UInt8, d DateTime('UTC'), e DateTime('UTC') MATERIALIZED d)
ENGINE = MergeTree ORDER BY k TTL e + INTERVAL 1 SECOND
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t_rows_materialized;
INSERT INTO t_rows_materialized (k, d) VALUES (1, '2020-01-01 00:00:00'), (2, '2020-01-01 00:00:00');
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_rows_materialized' AND active;
ALTER TABLE t_rows_materialized UPDATE d = toDateTime('2090-01-01 00:00:00', 'UTC') WHERE k = 1 SETTINGS mutations_sync = 2;
SELECT 'rows_materialized', k, e FROM t_rows_materialized ORDER BY k;

-- `DELETE WHERE` rule: the UPDATE moves a matching row past the expired bound; the rule must still apply to it later.
CREATE TABLE t_rows_where (k UInt8, d DateTime('UTC'), f UInt8)
ENGINE = MergeTree ORDER BY k TTL d + INTERVAL 1 SECOND DELETE WHERE f = 1
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
SYSTEM STOP TTL MERGES t_rows_where;
INSERT INTO t_rows_where VALUES (1, '2020-01-01 00:00:00', 1), (2, '2020-01-01 00:00:00', 1), (3, '2020-01-01 00:00:00', 0);
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_rows_where' AND active;
ALTER TABLE t_rows_where UPDATE d = now() WHERE k = 1 SETTINGS mutations_sync = 2;
SELECT 'rows_where', k FROM t_rows_where ORDER BY k;
SELECT sleep(2) FORMAT Null;
SYSTEM START TTL MERGES t_rows_where;
OPTIMIZE TABLE t_rows_where FINAL;
SELECT 'rows_where_merged', k FROM t_rows_where ORDER BY k;

DROP TABLE t_col_wide;
DROP TABLE t_col_compact;
DROP TABLE t_col_wide_partial;
DROP TABLE t_rows_wide;
DROP TABLE t_rows_compact;
DROP TABLE t_rows_drop_parts;
DROP TABLE t_rows_materialized;
DROP TABLE t_rows_where;
