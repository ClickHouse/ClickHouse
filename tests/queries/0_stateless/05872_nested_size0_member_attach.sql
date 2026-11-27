-- With shared offsets, a Nested column named `size0` takes the name of the array sizes the columns
-- of the Nested share. It is rejected on CREATE and ALTER; a table that already has one still
-- attaches, its columns read as before, and reading the array sizes of the other columns fails
-- with a clear error, in wide parts, until it is renamed.

-- The attach logs the incorrect definition.
SET send_logs_level = 'fatal';
SET allow_deprecated_database_ordinary = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
-- A full ATTACH needs a UUID in an Atomic database.
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;

ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_wide (`n.size0` Array(UInt64), `n.x` Array(String))
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
ATTACH TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_compact (`n.size0` Array(UInt64), `n.x` Array(String))
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = '10G';
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_wide VALUES ([100, 200], ['a', 'b']);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t_compact VALUES ([100, 200], ['a', 'b']);

SELECT 'wide', n.size0, n.x FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_wide;
SELECT n.x.size0 FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_wide; -- { serverError ILLEGAL_COLUMN }
SELECT length(n.x) FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_wide SETTINGS optimize_functions_to_subcolumns = 1; -- { serverError ILLEGAL_COLUMN }
SELECT 'compact', n.size0, n.x, n.x.size0 FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_compact;

-- An ALTER that keeps the column is rejected, a rename that removes it is not.
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_wide ADD COLUMN y UInt8; -- { serverError BAD_ARGUMENTS }
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t_wide RENAME COLUMN `n.size0` TO `n.sz`;
SELECT 'renamed', n.sz, n.x, n.x.size0 FROM {CLICKHOUSE_DATABASE_1:Identifier}.t_wide;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
