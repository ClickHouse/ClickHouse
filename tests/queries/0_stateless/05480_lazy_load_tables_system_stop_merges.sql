-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.
-- no-shared-merge-tree: the table engine is replaced.

-- `SYSTEM STOP MERGES` naming a table that is not loaded yet has to take the lock: otherwise it does nothing,
-- and merges run as soon as the table is first accessed.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (2);

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'stand-in', engine, is_loaded FROM system.tables WHERE database = currentDatabase() AND name = 't';
SYSTEM STOP MERGES {CLICKHOUSE_DATABASE_1:Identifier}.t;
SELECT 'stopped', engine, is_loaded FROM system.tables WHERE database = currentDatabase() AND name = 't';

OPTIMIZE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t FINAL; -- { serverError ABORTED }
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;

SYSTEM START MERGES {CLICKHOUSE_DATABASE_1:Identifier}.t;
OPTIMIZE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t FINAL;
SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
