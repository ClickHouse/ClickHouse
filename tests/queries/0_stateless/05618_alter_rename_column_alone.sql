-- With `allow_combining_rename_column_with_other_alter_commands` disabled, an ALTER query with `RENAME COLUMN`
-- must consist of that single command: a rename is a mutation of its own.

DROP TABLE IF EXISTS t_rename_alone;

CREATE TABLE t_rename_alone (k UInt64, a UInt64, b String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_rename_alone VALUES (1, 10, 'x');

SET allow_combining_rename_column_with_other_alter_commands = 0;

ALTER TABLE t_rename_alone RENAME COLUMN a TO a1, ADD COLUMN a UInt8; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_rename_alone DROP COLUMN b, RENAME COLUMN a TO a1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_rename_alone RENAME COLUMN a TO a1, MODIFY COLUMN b LowCardinality(String); -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_rename_alone RENAME COLUMN a TO a1, ADD INDEX i b TYPE minmax; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_rename_alone RENAME COLUMN a TO a1, RENAME COLUMN b TO b1; -- { serverError BAD_ARGUMENTS }

-- A command that `IF EXISTS` or `IF NOT EXISTS` turns into a no-op does not count.
ALTER TABLE t_rename_alone RENAME COLUMN IF EXISTS missing TO m, ADD COLUMN c UInt8;
ALTER TABLE t_rename_alone RENAME COLUMN a TO a1, ADD COLUMN IF NOT EXISTS c UInt8;

ALTER TABLE t_rename_alone RENAME COLUMN a1 TO a2;

-- With the setting enabled (the default), the combination is allowed.
ALTER TABLE t_rename_alone RENAME COLUMN a2 TO a3, RENAME COLUMN b TO b2, ADD COLUMN d UInt8 SETTINGS allow_combining_rename_column_with_other_alter_commands = 1;

SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 't_rename_alone' ORDER BY position;
SELECT * FROM t_rename_alone;

DROP TABLE t_rename_alone;
