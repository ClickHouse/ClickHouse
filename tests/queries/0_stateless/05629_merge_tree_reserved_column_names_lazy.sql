-- Tags: no-replicated-database
-- no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.

-- The table an ALTER lands on may not be loaded yet; its virtual columns must still be seen.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_alias ENGINE = Alias({CLICKHOUSE_DATABASE_1:String}, 't');

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ALTER TABLE t_alias ADD COLUMN _part_offset UInt64; -- { serverError ILLEGAL_COLUMN }

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t ADD COLUMN _part UInt32; -- { serverError ILLEGAL_COLUMN }

DESCRIBE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t SETTINGS describe_compact_output = 1;
DROP TABLE t_alias;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
