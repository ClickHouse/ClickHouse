-- Tags: no-replicated-database
-- Tag no-replicated-database: tables of `Replicated` databases are not reattached.

-- `reattach_tables_before_query_execution_probability` must keep the action locks taken on an `Alias` in another database:
-- `SYSTEM STOP MERGES` on the alias stops merges of its target, which is a table of the current database.

CREATE TABLE mt (x UInt64) ENGINE = MergeTree ORDER BY x;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.alias_to_mt ENGINE = Alias({CLICKHOUSE_DATABASE:String}, mt);

SYSTEM STOP MERGES {CLICKHOUSE_DATABASE_1:Identifier}.alias_to_mt;
INSERT INTO mt VALUES (1);
INSERT INTO mt VALUES (2);

SELECT 1 SETTINGS reattach_tables_before_query_execution_probability = 1;

OPTIMIZE TABLE mt FINAL SETTINGS optimize_throw_if_noop = 1; -- { serverError ABORTED }
SELECT 'alias in another database, stopped merges', count() FROM system.parts WHERE database = currentDatabase() AND table = 'mt' AND active;

SYSTEM START MERGES {CLICKHOUSE_DATABASE_1:Identifier}.alias_to_mt;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
