-- Regression test: the arguments of a table engine are evaluated by the storage factory against the current
-- database of the session, so the check for cyclic dependencies must resolve them the same way, even when
-- the table is created in another database. Otherwise a cycle of `Buffer` tables passes the check and
-- is registered in the dependency graph.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

CREATE TABLE buffer_cycle_a (id UInt64) ENGINE = Buffer({CLICKHOUSE_DATABASE_1:String}, 'buffer_cycle_b', 1, 0, 1000, 0, 1000000, 0, 1000000);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.buffer_cycle_b (id UInt64) ENGINE = Buffer('', 'buffer_cycle_a', 1, 0, 1000, 0, 1000000, 0, 1000000); -- { serverError INFINITE_LOOP }

CREATE TABLE buffer_cycle_c (id UInt64) ENGINE = Buffer({CLICKHOUSE_DATABASE_1:String}, 'buffer_cycle_d', 1, 0, 1000, 0, 1000000, 0, 1000000);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.buffer_cycle_d (id UInt64) ENGINE = Buffer(currentDatabase(), 'buffer_cycle_c', 1, 0, 1000, 0, 1000000, 0, 1000000); -- { serverError INFINITE_LOOP }

-- No cycle was registered, so an unrelated table with dependencies can be created.
CREATE VIEW buffer_cycle_unrelated AS SELECT * FROM system.one;

SELECT database = currentDatabase(), name FROM system.tables
WHERE database IN (currentDatabase(), {CLICKHOUSE_DATABASE_1:String}) AND name LIKE 'buffer_cycle_%'
ORDER BY name;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
