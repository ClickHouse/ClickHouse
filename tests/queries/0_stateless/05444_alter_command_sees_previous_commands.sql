-- A command of an ALTER is prepared against the columns as the previous commands of the statement leave them.
-- https://github.com/ClickHouse/ClickHouse/issues/127444

DROP TABLE IF EXISTS t_alter_previous_commands;
CREATE TABLE t_alter_previous_commands (id UInt64, p Tuple(a UInt8, b UInt8), arr Array(UInt8)) ENGINE = MergeTree ORDER BY id;

-- A column added by the statement is the target of a later MODIFY, not the subcolumn with the same name.
ALTER TABLE t_alter_previous_commands ADD COLUMN `p.b` Map(String, UInt8), MODIFY COLUMN `p.b` Map(String, UInt16);
ALTER TABLE t_alter_previous_commands ADD COLUMN `arr.size0` UInt64, MODIFY COLUMN `arr.size0` UInt32 COMMENT 'c';
SELECT name, type, comment FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_previous_commands' AND name IN ('p.b', 'arr.size0') ORDER BY position;

-- The same for a codec: it goes to the added column, not to the element of the tuple that the statement drops.
ALTER TABLE t_alter_previous_commands ADD COLUMN `p.a` UInt64, MODIFY COLUMN `p.a` CODEC(LZ4), DROP COLUMN p;
SELECT name, type, compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_previous_commands' AND name = 'p.a';

-- IF EXISTS and IF NOT EXISTS see the columns added and dropped by the previous commands.
ALTER TABLE t_alter_previous_commands ADD COLUMN q UInt8, MODIFY COLUMN IF EXISTS q UInt16 COMMENT 'modified';
SELECT name, type, comment FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_previous_commands' AND name = 'q';
ALTER TABLE t_alter_previous_commands DROP COLUMN q, ADD COLUMN IF NOT EXISTS q String;
SELECT name, type, comment FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_previous_commands' AND name = 'q';

-- ADD ENUM VALUES extends the type of the column added by the previous command.
ALTER TABLE t_alter_previous_commands ADD COLUMN e Enum8('a' = 1), MODIFY COLUMN e ADD ENUM VALUES('b' = 2);
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_previous_commands' AND name = 'e';

-- IF NOT EXISTS sees that the previous command renamed the column away.
ALTER TABLE t_alter_previous_commands RENAME COLUMN q TO r, ADD COLUMN IF NOT EXISTS q Int32;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_previous_commands' AND name IN ('q', 'r') ORDER BY position;

SHOW CREATE TABLE t_alter_previous_commands;
DROP TABLE t_alter_previous_commands;

-- A column added by the statement keeps its ALIAS kind for the later commands: an ALIAS has no subcolumns to modify.
CREATE TABLE t_alter_previous_commands_alias (id UInt64) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_alter_previous_commands_alias ADD COLUMN p Tuple(a UInt32) ALIAS tuple(toUInt32(id)), MODIFY COLUMN IF EXISTS `p.a` CODEC(LZ4) SETTINGS enable_subcolumn_codecs = 1;
SELECT name, type, default_kind, compression_codec FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_previous_commands_alias' ORDER BY position;
SELECT position(create_table_query, 'CODEC') FROM system.tables WHERE database = currentDatabase() AND name = 't_alter_previous_commands_alias';
DROP TABLE t_alter_previous_commands_alias;

-- The placement of a column (FIRST / AFTER) decides which columns DROP COLUMN of a Nested group removes: `n.b` placed
-- after `n.a` is dropped together with `n`, so the following DROP COLUMN `n.b` finds no such column.
CREATE TABLE t_alter_previous_commands_nested (`n.a` Array(UInt32), id UInt64) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_alter_previous_commands_nested ADD COLUMN `n.b` Array(UInt32) AFTER `n.a`, DROP COLUMN n, DROP COLUMN `n.b`; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
DROP TABLE t_alter_previous_commands_nested;

CREATE TABLE t_alter_previous_commands_nested (`n.a` Array(UInt32), id UInt64, `n.b` Array(UInt32)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_alter_previous_commands_nested MODIFY COLUMN `n.b` Array(UInt32) AFTER `n.a`, DROP COLUMN n, DROP COLUMN `n.b`; -- { serverError NOT_FOUND_COLUMN_IN_BLOCK }
DROP TABLE t_alter_previous_commands_nested;

-- DROP COLUMN IF EXISTS of a Nested group drops the group like DROP COLUMN does, so the metadata agrees with the mutation
-- and the data of the group is not lost while its columns stay.
CREATE TABLE t_alter_previous_commands_nested (id UInt64, `n.a` Array(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alter_previous_commands_nested VALUES (1, [42]);
ALTER TABLE t_alter_previous_commands_nested DROP COLUMN IF EXISTS n;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_previous_commands_nested' ORDER BY position;
SELECT * FROM t_alter_previous_commands_nested;
DROP TABLE t_alter_previous_commands_nested;

CREATE TABLE t_alter_previous_commands_nested (id UInt64, `n.a` Array(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alter_previous_commands_nested VALUES (1, [42]);
ALTER TABLE t_alter_previous_commands_nested DROP COLUMN IF EXISTS n, DROP COLUMN IF EXISTS `n.a`;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_previous_commands_nested' ORDER BY position;
SELECT * FROM t_alter_previous_commands_nested;
DROP TABLE t_alter_previous_commands_nested;

-- The mutation of DROP COLUMN of a Nested group drops each column of the group, so a column of the group created again
-- by the same ALTER starts empty in a wide part instead of keeping the data of the dropped column.
CREATE TABLE t_alter_previous_commands_nested (id UInt64, n Nested(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY id SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_alter_previous_commands_nested VALUES (1, [11], [42]);
ALTER TABLE t_alter_previous_commands_nested DROP COLUMN n, ADD COLUMN IF NOT EXISTS `n.b` Array(UInt64);
SELECT id, `n.b` FROM t_alter_previous_commands_nested;
SELECT column, type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_alter_previous_commands_nested' AND active ORDER BY column;
DROP TABLE t_alter_previous_commands_nested;

CREATE TABLE t_alter_previous_commands_nested (id UInt64, n Nested(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY id SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_alter_previous_commands_nested VALUES (1, [11], [42]);
ALTER TABLE t_alter_previous_commands_nested DROP COLUMN n, ADD COLUMN n Nested(b UInt64);
SELECT id, `n.b` FROM t_alter_previous_commands_nested;
DROP TABLE t_alter_previous_commands_nested;

CREATE TABLE t_alter_previous_commands_nested (n Nested(a UInt32), id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_alter_previous_commands_nested VALUES ([42], 1);
ALTER TABLE t_alter_previous_commands_nested ADD COLUMN `n.b` Array(UInt32), DROP COLUMN n, RENAME COLUMN IF EXISTS `n.b` TO `n.a`;
SELECT id, `n.a` FROM t_alter_previous_commands_nested;
DROP TABLE t_alter_previous_commands_nested;

-- A column dropped and added again by the same ALTER cannot be renamed in it: the mutation would rename the data of the
-- dropped column instead of removing it.
CREATE TABLE t_alter_previous_commands_readd (id UInt64, q UInt32) ENGINE = MergeTree ORDER BY id SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_alter_previous_commands_readd VALUES (1, 42);
ALTER TABLE t_alter_previous_commands_readd DROP COLUMN q, ADD COLUMN IF NOT EXISTS q UInt32 DEFAULT 7, RENAME COLUMN IF EXISTS q TO r; -- { serverError NOT_IMPLEMENTED }
SELECT id, q FROM t_alter_previous_commands_readd;
DROP TABLE t_alter_previous_commands_readd;

CREATE TABLE t_alter_previous_commands_readd (id UInt64, n Nested(a UInt32)) ENGINE = MergeTree ORDER BY id SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_alter_previous_commands_readd VALUES (1, [42]);
ALTER TABLE t_alter_previous_commands_readd DROP COLUMN n, ADD COLUMN n Nested(a UInt32), RENAME COLUMN `n.a` TO `n.c`; -- { serverError NOT_IMPLEMENTED }
SELECT id, `n.a` FROM t_alter_previous_commands_readd;
DROP TABLE t_alter_previous_commands_readd;
