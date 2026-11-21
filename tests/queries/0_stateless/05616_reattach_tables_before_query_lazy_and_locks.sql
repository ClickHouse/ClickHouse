-- Tags: no-replicated-database
-- Tag no-replicated-database: tables of `Replicated` databases are not reattached.

-- `reattach_tables_before_query_execution_probability` must keep the action locks taken on a materialized view:
-- `SYSTEM STOP MERGES mv` stops merges of its inner table, which is a separate table of the same database.

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY x AS SELECT x FROM src;

SYSTEM STOP MERGES mv;
INSERT INTO src VALUES (1);
INSERT INTO src VALUES (2);

SELECT 1 SETTINGS reattach_tables_before_query_execution_probability = 1;

OPTIMIZE TABLE mv FINAL SETTINGS optimize_throw_if_noop = 1; -- { serverError ABORTED }
SELECT 'materialized view, stopped merges', count() FROM system.parts WHERE database = currentDatabase() AND table LIKE '.inner%' AND active;

-- In a database with `lazy_load_tables`, the tables are proxies. `Set` and `Join` with `persistent = 0` behind them must be skipped.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.s (x UInt64) ENGINE = Set SETTINGS persistent = 0;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.j (x UInt64, v String) ENGINE = Join(ANY, LEFT, x) SETTINGS persistent = 0;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.mt (x UInt64) ENGINE = MergeTree ORDER BY x;
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.s VALUES (42);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.j VALUES (42, 'present');
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.mt VALUES (42);

USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT 1 SETTINGS reattach_tables_before_query_execution_probability = 1;

SELECT 'lazy Set, persistent = 0', 42 IN s;
SELECT 'lazy Join, persistent = 0', count() FROM j;
SELECT 'lazy MergeTree', count() FROM mt;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
