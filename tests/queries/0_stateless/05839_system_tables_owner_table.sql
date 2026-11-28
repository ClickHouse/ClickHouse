-- Tests the `owner_table` column of `system.tables`: for an inner table, the materialized view or the `TimeSeries` table owning it.

SET allow_experimental_time_series_table = 1;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE metrics ENGINE = TimeSeries;
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM src;
-- A view writing to a `TO` table has no inner table: `dst` has no owner.
CREATE MATERIALIZED VIEW mv_to TO dst AS SELECT x FROM src;

SELECT 'Atomic:';
SELECT replaceRegexpOne(name, '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', '<uuid>') AS table, owner_table
FROM system.tables WHERE database = currentDatabase() ORDER BY owner_table, table;

-- The inner tables keep their names, and `owner_table` shows the new names of the renamed tables.
RENAME TABLE metrics TO metrics_renamed;
RENAME TABLE mv TO mv_renamed;

SELECT 'Atomic, after RENAME:';
SELECT replaceRegexpOne(name, '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', '<uuid>') AS table, owner_table
FROM system.tables WHERE database = currentDatabase() ORDER BY owner_table, table;

-- With a filter by name only the requested table is listed, and its owner is found anyway.
SELECT 'filter by name:';
SELECT owner_table FROM system.tables
WHERE database = currentDatabase() AND name = (SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.tags.%');

-- Tables only named like inner tables: after an unknown or the nil UUID, after a table which is not a view,
-- or after a view and a `TimeSeries` table which don't own them.
CREATE TABLE `.inner_id.11111111-2222-3333-4444-555555555555` (x UInt64) ENGINE = Memory;
CREATE TABLE `.inner_id.00000000-0000-0000-0000-000000000000` (x UInt64) ENGINE = Memory;
CREATE TABLE `.inner.src` (x UInt64) ENGINE = Memory;
CREATE TABLE `.inner.mv_renamed` (x UInt64) ENGINE = Memory;
CREATE TABLE `.inner.samples.metrics_renamed` (x UInt64) ENGINE = Memory;

SELECT 'Atomic, no owner:';
SELECT name, owner_table FROM system.tables WHERE database = currentDatabase() AND engine = 'Memory' ORDER BY name;

-- In an Ordinary database the inner tables are named after their owner, and `RENAME` renames them too.
SET allow_deprecated_database_ordinary = 1;
SET send_logs_level = 'fatal'; -- the server warns that the Ordinary engine is deprecated
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.metrics ENGINE = TimeSeries;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;
-- Only named like inner tables: `src` is not a view, and `mv` doesn't own `.inner.tags.mv`.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.`.inner.src` (x UInt64) ENGINE = Memory;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.`.inner.tags.mv` (x UInt64) ENGINE = Memory;

SELECT 'Ordinary:';
SELECT name, owner_table FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} ORDER BY owner_table, name;

RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.metrics TO {CLICKHOUSE_DATABASE_1:Identifier}.metrics_renamed;
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.mv TO {CLICKHOUSE_DATABASE_1:Identifier}.mv_renamed;

SELECT 'Ordinary, after RENAME:';
SELECT name, owner_table FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} ORDER BY owner_table, name;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
