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

-- The placement of a column (FIRST / AFTER) decides which columns DROP COLUMN of a Nested group removes,
-- so a later IF NOT EXISTS sees the same columns as `apply` does.
CREATE TABLE t_alter_previous_commands_nested (`n.a` Array(UInt32), id UInt64) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_alter_previous_commands_nested ADD COLUMN `n.b` Array(UInt32) AFTER `n.a`, DROP COLUMN n, ADD COLUMN IF NOT EXISTS `n.b` Array(UInt64);
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_previous_commands_nested' ORDER BY position;
DROP TABLE t_alter_previous_commands_nested;

CREATE TABLE t_alter_previous_commands_nested (`n.a` Array(UInt32), id UInt64, `n.b` Array(UInt32)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_alter_previous_commands_nested MODIFY COLUMN `n.b` Array(UInt32) AFTER `n.a`, DROP COLUMN n, ADD COLUMN IF NOT EXISTS `n.b` Array(UInt64);
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_previous_commands_nested' ORDER BY position;
DROP TABLE t_alter_previous_commands_nested;

-- DROP COLUMN IF EXISTS of a Nested group drops the group like DROP COLUMN does, so the metadata agrees with the mutation
-- and the data of the group is not lost while its columns stay.
CREATE TABLE t_alter_previous_commands_nested (id UInt64, `n.a` Array(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alter_previous_commands_nested VALUES (1, [42]);
ALTER TABLE t_alter_previous_commands_nested DROP COLUMN IF EXISTS n SETTINGS mutations_sync = 1;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_previous_commands_nested' ORDER BY position;
SELECT * FROM t_alter_previous_commands_nested;
DROP TABLE t_alter_previous_commands_nested;

CREATE TABLE t_alter_previous_commands_nested (id UInt64, `n.a` Array(UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alter_previous_commands_nested VALUES (1, [42]);
ALTER TABLE t_alter_previous_commands_nested DROP COLUMN IF EXISTS n, DROP COLUMN IF EXISTS `n.a` SETTINGS mutations_sync = 1;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_alter_previous_commands_nested' ORDER BY position;
SELECT * FROM t_alter_previous_commands_nested;
DROP TABLE t_alter_previous_commands_nested;
