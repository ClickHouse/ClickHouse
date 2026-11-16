-- A JSON column filled with the default of its type holds the default of each typed path: an Enum gets its first value, not 0.
DROP TABLE IF EXISTS t_json_default_omitted;
DROP TABLE IF EXISTS t_json_default_add_column;
DROP TABLE IF EXISTS t_json_default_ttl_wide;
DROP TABLE IF EXISTS t_json_default_ttl_compact;
DROP TABLE IF EXISTS t_json_default_skip_empty;

CREATE TABLE t_json_default_omitted (id UInt32, j JSON(a Enum8('x' = 1, 'y' = 2), z Enum8('m' = -3, 'n' = 0), t Tuple(n UInt8, s Enum8('a' = 1, 'b' = 2)), o JSON(e Enum8('p' = 5, 'q' = 6)), d Date32))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_default_omitted (id) VALUES (1);
SELECT 'omitted', id, j, toInt8(j.a), toInt8(j.z) FROM t_json_default_omitted;
SELECT 'parsed', '{}'::JSON(a Enum8('x' = 1, 'y' = 2), z Enum8('m' = -3, 'n' = 0), t Tuple(n UInt8, s Enum8('a' = 1, 'b' = 2)), o JSON(e Enum8('p' = 5, 'q' = 6)), d Date32);
SELECT 'default', defaultValueOfArgumentType(j) FROM t_json_default_omitted;

CREATE TABLE t_json_default_add_column (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json_default_add_column VALUES (1);
ALTER TABLE t_json_default_add_column ADD COLUMN j JSON(a Enum8('x' = 1, 'y' = 2));
SELECT 'add column', id, j, j.a FROM t_json_default_add_column;
INSERT INTO t_json_default_add_column VALUES (2, '{"a":"y"}');
ALTER TABLE t_json_default_add_column CLEAR COLUMN j SETTINGS mutations_sync = 2;
SELECT 'clear column', id, j FROM t_json_default_add_column ORDER BY id;

CREATE TABLE t_json_default_ttl_wide (id UInt32, d DateTime, j JSON(a Enum8('x' = 1, 'y' = 2)) TTL d + INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_json_default_ttl_wide VALUES (1, '2000-01-01 00:00:00', '{"a":"y"}');
OPTIMIZE TABLE t_json_default_ttl_wide FINAL;
CREATE TABLE t_json_default_ttl_compact (id UInt32, d DateTime, j JSON(a Enum8('x' = 1, 'y' = 2)) TTL d + INTERVAL 1 DAY)
ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_json_default_ttl_compact VALUES (1, '2000-01-01 00:00:00', '{"a":"y"}');
OPTIMIZE TABLE t_json_default_ttl_compact FINAL;
SELECT 'ttl wide', id, j FROM t_json_default_ttl_wide;
SELECT 'ttl compact', id, j FROM t_json_default_ttl_compact;

CREATE TABLE t_json_default_skip_empty (id UInt32, j JSON(z Enum8('neg' = -1, 'zero' = 0, 'pos' = 1)))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
SYSTEM STOP MERGES t_json_default_skip_empty;
-- The all-zero value is skipped and must read back as 'zero', also after a merge with a part that stores j.
INSERT INTO t_json_default_skip_empty VALUES (1, '{"z":"zero"}');
INSERT INTO t_json_default_skip_empty (id) VALUES (2);
SELECT 'skip empty parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_json_default_skip_empty' AND active;
SELECT 'skip empty parts with j', count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_json_default_skip_empty' AND active AND column = 'j';
SELECT 'skip empty', id, j FROM t_json_default_skip_empty ORDER BY id;
SYSTEM START MERGES t_json_default_skip_empty;
OPTIMIZE TABLE t_json_default_skip_empty FINAL;
SELECT 'skip empty merged parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_json_default_skip_empty' AND active;
SELECT 'skip empty merged', id, j FROM t_json_default_skip_empty ORDER BY id;

SELECT 'type name', defaultValueOfTypeName('JSON(a Enum8(\'x\' = 1, \'y\' = 2))');

DROP TABLE t_json_default_omitted;
DROP TABLE t_json_default_add_column;
DROP TABLE t_json_default_ttl_wide;
DROP TABLE t_json_default_ttl_compact;
DROP TABLE t_json_default_skip_empty;
