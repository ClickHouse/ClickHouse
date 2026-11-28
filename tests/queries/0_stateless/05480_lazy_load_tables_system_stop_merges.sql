-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database: the database engine is replaced, which drops the `lazy_load_tables` setting.
-- no-shared-merge-tree: the table engine is replaced.

-- `SYSTEM STOP MERGES` naming a table that is not loaded yet has to take the lock: otherwise it does nothing,
-- and merges run as soon as the table is first accessed.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
-- Background merges would leave one part before the lock is taken; the explicit `OPTIMIZE` still exercises the lock.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS merge_selector_algorithm = 'Manual';
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

-- A materialized view and an `Alias` are loaded eagerly and forward the lock to a table that is not:
-- the inner table of the view and the target of the alias. Naming the wrapper has to load that table as well.
SET allow_experimental_alias_table_engine = 1;
CREATE TABLE src (x UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY x SETTINGS merge_selector_algorithm = 'Manual' AS SELECT x FROM src;
INSERT INTO src VALUES (1);
INSERT INTO src VALUES (2);
CREATE TABLE target (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS merge_selector_algorithm = 'Manual';
INSERT INTO target VALUES (1);
INSERT INTO target VALUES (2);
CREATE TABLE alias ENGINE = Alias({CLICKHOUSE_DATABASE_1:Identifier}, target);

USE {CLICKHOUSE_DATABASE:Identifier};
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT 'stand-in', replaceRegexpOne(name, '^\\.inner_id\\..*', 'inner'), engine, is_loaded FROM system.tables WHERE database = currentDatabase() AND name NOT IN ('t', 'src') ORDER BY ALL;
SYSTEM STOP MERGES mv;
SYSTEM STOP MERGES alias;
SELECT 'stopped', replaceRegexpOne(name, '^\\.inner_id\\..*', 'inner'), engine, is_loaded FROM system.tables WHERE database = currentDatabase() AND name NOT IN ('t', 'src') ORDER BY ALL;

OPTIMIZE TABLE mv FINAL; -- { serverError ABORTED }
OPTIMIZE TABLE alias FINAL; -- { serverError ABORTED }
SELECT 'parts', replaceRegexpOne(table, '^\\.inner_id\\..*', 'inner') AS name, count() FROM system.parts WHERE database = currentDatabase() AND table != 't' AND active GROUP BY name ORDER BY name;

SYSTEM START MERGES mv;
SYSTEM START MERGES alias;
OPTIMIZE TABLE mv FINAL;
OPTIMIZE TABLE alias FINAL;
SELECT 'parts', replaceRegexpOne(table, '^\\.inner_id\\..*', 'inner') AS name, count() FROM system.parts WHERE database = currentDatabase() AND table != 't' AND active GROUP BY name ORDER BY name;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
