-- Tests the `parent_table` column of `system.tables`: for an inner table, the materialized view or the `TimeSeries` table owning it.

SET allow_experimental_time_series_table = 1;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE metrics ENGINE = TimeSeries;
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM src;
-- A view writing to a `TO` table has no inner table: `dst` has no parent.
CREATE MATERIALIZED VIEW mv_to TO dst AS SELECT x FROM src;

SELECT 'Atomic:';
SELECT replaceRegexpOne(name, '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', '<uuid>') AS table, parent_table
FROM system.tables WHERE database = currentDatabase() ORDER BY parent_table, table;

-- The inner tables keep their names, and `parent_table` shows the new names of the renamed tables.
RENAME TABLE metrics TO metrics_renamed;
RENAME TABLE mv TO mv_renamed;

SELECT 'Atomic, after RENAME:';
SELECT replaceRegexpOne(name, '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', '<uuid>') AS table, parent_table
FROM system.tables WHERE database = currentDatabase() ORDER BY parent_table, table;

-- With a filter by name only the requested table is listed, and its parent is found anyway.
SELECT 'filter by name:';
SELECT parent_table FROM system.tables
WHERE database = currentDatabase() AND name = (SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.tags.%');

-- In an Ordinary database the inner tables are named after their parent, and `RENAME` renames them too.
SET allow_deprecated_database_ordinary = 1;
SET send_logs_level = 'fatal'; -- the server warns that the Ordinary engine is deprecated
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.metrics ENGINE = TimeSeries;
CREATE MATERIALIZED VIEW {CLICKHOUSE_DATABASE_1:Identifier}.mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.src;

SELECT 'Ordinary:';
SELECT name, parent_table FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} ORDER BY parent_table, name;

RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.metrics TO {CLICKHOUSE_DATABASE_1:Identifier}.metrics_renamed;
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.mv TO {CLICKHOUSE_DATABASE_1:Identifier}.mv_renamed;

SELECT 'Ordinary, after RENAME:';
SELECT name, parent_table FROM system.tables WHERE database = {CLICKHOUSE_DATABASE_1:String} ORDER BY parent_table, name;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
