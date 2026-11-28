-- Tags: no-replicated-database
-- no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.

-- `TRUNCATE TABLE IF EMPTY` and `DROP TABLE IF EMPTY` of an `Alias` count the rows of its target. A deferred
-- table of a `lazy_load_tables` database does not know its row count until it is loaded, so the target is loaded
-- to count them: an unknown count must not let a nonempty table be truncated through the alias.

SET allow_experimental_alias_table_engine = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.nonempty (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.empty (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.nonempty VALUES (7);
CREATE TABLE alias_nonempty ENGINE = Alias({CLICKHOUSE_DATABASE_1:Identifier}, nonempty);
CREATE TABLE alias_empty ENGINE = Alias({CLICKHOUSE_DATABASE_1:Identifier}, empty);

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT name, is_loaded FROM system.tables WHERE database = currentDatabase() || '_1' ORDER BY name;

TRUNCATE TABLE IF EMPTY alias_nonempty; -- { serverError TABLE_NOT_EMPTY }
SELECT name, is_loaded FROM system.tables WHERE database = currentDatabase() || '_1' ORDER BY name;
SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.nonempty;

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE IF EMPTY alias_nonempty; -- { serverError TABLE_NOT_EMPTY }
EXISTS TABLE alias_nonempty;

TRUNCATE TABLE IF EMPTY alias_empty;
DROP TABLE IF EMPTY alias_empty;
EXISTS TABLE alias_empty;

DROP TABLE alias_nonempty;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
