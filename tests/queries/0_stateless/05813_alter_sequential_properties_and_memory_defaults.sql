-- Tags: no-replicated-database
-- Some sequential property commands mix replicated and non-replicated ALTER types.
-- A column added after the insert reads as the type default. The replacement default uses that value.
DROP TABLE IF EXISTS t_mem_added_default;
CREATE TABLE t_mem_added_default (id UInt8, x UInt64) ENGINE = Memory;
INSERT INTO t_mem_added_default VALUES (1, 7);
ALTER TABLE t_mem_added_default ADD COLUMN a UInt64 DEFAULT 257;
SELECT a, a + 1 FROM t_mem_added_default;
ALTER TABLE t_mem_added_default DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT a + 1;
SELECT a, x, a + 1 FROM t_mem_added_default;
DROP TABLE t_mem_added_default;

-- An unrelated column whose stored value cannot fill its new type does not reject `CLEAR` of another column.
DROP TABLE IF EXISTS t_mem_unrelated_convert;
CREATE TABLE t_mem_unrelated_convert (a String, x UInt64) ENGINE = Memory;
INSERT INTO t_mem_unrelated_convert VALUES ('bad', 7);
ALTER TABLE t_mem_unrelated_convert MODIFY COLUMN a UInt8;
ALTER TABLE t_mem_unrelated_convert CLEAR COLUMN x;
SELECT x FROM t_mem_unrelated_convert;
DROP TABLE t_mem_unrelated_convert;

-- An `ALIAS` tuple is a valid parent of a subcolumn in a replacement default.
DROP TABLE IF EXISTS t_mem_alias_subcolumn;
CREATE TABLE t_mem_alias_subcolumn (id UInt64, a Tuple(v UInt64) ALIAS tuple(id + 100), x UInt64) ENGINE = Memory;
INSERT INTO t_mem_alias_subcolumn (id, x) VALUES (1, 0);
SELECT a.v + 1 FROM t_mem_alias_subcolumn;
ALTER TABLE t_mem_alias_subcolumn DROP COLUMN x, ADD COLUMN IF NOT EXISTS x UInt64 DEFAULT a.v + 1;
SELECT id, a.v, x FROM t_mem_alias_subcolumn;
DROP TABLE t_mem_alias_subcolumn;

-- Later commands of one `ALTER` see properties and defaults the earlier commands introduced.
DROP TABLE IF EXISTS t_alter_props;
CREATE TABLE t_alter_props (x UInt8, y UInt8) ENGINE = Memory;
ALTER TABLE t_alter_props ADD COLUMN z UInt8 DEFAULT 7, MODIFY COLUMN z REMOVE DEFAULT;
ALTER TABLE t_alter_props MODIFY COLUMN x COMMENT 'c', MODIFY COLUMN x REMOVE COMMENT;
ALTER TABLE t_alter_props COMMENT COLUMN x 'c', MODIFY COLUMN x REMOVE COMMENT;
ALTER TABLE t_alter_props MODIFY COLUMN x DEFAULT y, MODIFY COLUMN x DEFAULT 1, DROP COLUMN y;
ALTER TABLE t_alter_props ADD COLUMN y UInt8, MODIFY COLUMN x DEFAULT y, DROP COLUMN x, DROP COLUMN y;
SELECT name, default_kind, default_expression, comment FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_props' ORDER BY name;
DROP TABLE t_alter_props;

DROP TABLE IF EXISTS t_alter_rename_default;
CREATE TABLE t_alter_rename_default (x UInt8, y UInt8) ENGINE = Memory;
ALTER TABLE t_alter_rename_default MODIFY COLUMN x DEFAULT y, RENAME COLUMN y TO z;
SELECT name, default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_rename_default' ORDER BY name;
DROP TABLE t_alter_rename_default;

DROP TABLE IF EXISTS t_alter_remove_inherited_default;
CREATE TABLE t_alter_remove_inherited_default (x String DEFAULT 'a') ENGINE = Memory;
ALTER TABLE t_alter_remove_inherited_default MODIFY COLUMN x Int8, MODIFY COLUMN x REMOVE DEFAULT, MODIFY COLUMN x UInt8;
SELECT type, default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_remove_inherited_default' AND name = 'x';
DROP TABLE t_alter_remove_inherited_default;
