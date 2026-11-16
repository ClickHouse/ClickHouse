-- Tags: no-replicated-database
-- no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.

-- `DROP TABLE IF EMPTY` must load a deferred table of a `lazy_load_tables` database to count its rows:
-- the stand-in does not know the count, and an unknown count must not let a nonempty table be dropped.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.nonempty (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.empty (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.nonempty VALUES (7);

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT name, is_loaded FROM system.tables WHERE database = currentDatabase() || '_1' ORDER BY name;

DROP TABLE IF EMPTY {CLICKHOUSE_DATABASE_1:Identifier}.nonempty SYNC; -- { serverError TABLE_NOT_EMPTY }
SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.nonempty;

DROP TABLE IF EMPTY {CLICKHOUSE_DATABASE_1:Identifier}.empty SYNC;
EXISTS TABLE {CLICKHOUSE_DATABASE_1:Identifier}.empty;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
