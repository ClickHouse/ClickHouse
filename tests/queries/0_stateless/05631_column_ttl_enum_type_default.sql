-- A column TTL on an Enum column without DEFAULT must reset expired values to the first Enum value, not to 0.

DROP TABLE IF EXISTS t_column_ttl_enum_compact;
DROP TABLE IF EXISTS t_column_ttl_enum_wide;
DROP TABLE IF EXISTS t_column_ttl_enum_zero;
DROP TABLE IF EXISTS t_column_ttl_enum_wide_expired;

CREATE TABLE t_column_ttl_enum_compact (id UInt32, d DateTime, e Enum8('a' = 1, 'b' = 2) TTL d + INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1000000000;

INSERT INTO t_column_ttl_enum_compact VALUES (1, '2000-01-01 00:00:00', 'b'), (2, '2100-01-01 00:00:00', 'b');
INSERT INTO t_column_ttl_enum_compact VALUES (3, '2000-01-01 00:00:00', 'b'), (4, '2000-01-01 00:00:00', 'b');
OPTIMIZE TABLE t_column_ttl_enum_compact FINAL;
SELECT id, e, toInt8(e) FROM t_column_ttl_enum_compact ORDER BY id;

CREATE TABLE t_column_ttl_enum_wide (id UInt32, d DateTime, e Enum16('a' = 1, 'b' = 2) TTL d + INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_column_ttl_enum_wide VALUES (1, '2000-01-01 00:00:00', 'b'), (2, '2100-01-01 00:00:00', 'b');
OPTIMIZE TABLE t_column_ttl_enum_wide FINAL;
SELECT id, e, toInt16(e) FROM t_column_ttl_enum_wide ORDER BY id;

CREATE TABLE t_column_ttl_enum_zero (id UInt32, d DateTime, e Enum8('m' = -3, 'n' = 0, 'o' = 4) TTL d + INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_column_ttl_enum_zero VALUES (1, '2000-01-01 00:00:00', 'o'), (2, '2100-01-01 00:00:00', 'o');
OPTIMIZE TABLE t_column_ttl_enum_zero FINAL;
SELECT id, e FROM t_column_ttl_enum_zero ORDER BY id;

-- All rows of a wide part expire: the column is dropped from the part, but the projection is rebuilt from the reset values.
CREATE TABLE t_column_ttl_enum_wide_expired (id UInt32, d DateTime, e Enum8('a' = 1, 'b' = 2), PROJECTION p (SELECT id, e ORDER BY e))
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_column_ttl_enum_wide_expired VALUES (1, '2000-01-01 00:00:00', 'b'), (2, '2000-01-01 00:00:00', 'b');
SYSTEM STOP TTL MERGES t_column_ttl_enum_wide_expired;
ALTER TABLE t_column_ttl_enum_wide_expired MODIFY COLUMN e Enum8('a' = 1, 'b' = 2) TTL d + INTERVAL 1 DAY SETTINGS materialize_ttl_after_modify = 0;
ALTER TABLE t_column_ttl_enum_wide_expired MATERIALIZE TTL SETTINGS mutations_sync = 2;
SELECT id, toInt8(e) FROM mergeTreeProjection(currentDatabase(), t_column_ttl_enum_wide_expired, p) ORDER BY id;

DROP TABLE t_column_ttl_enum_compact;
DROP TABLE t_column_ttl_enum_wide;
DROP TABLE t_column_ttl_enum_zero;
DROP TABLE t_column_ttl_enum_wide_expired;
