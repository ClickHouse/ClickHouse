-- Regression test: the right argument of `IN` which is an alias of an expression is not a table, so it is neither
-- a dependency of a plain view nor a self-reference which blocks moving the view to a table of the same name.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Atomic;

CREATE TABLE ids (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE with_ids (id UInt64) ENGINE = MergeTree ORDER BY id;

CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v AS SELECT [1, 2] AS ids, toUInt64(1) IN ids AS present;
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v_with AS
    WITH [1, 2] AS with_ids SELECT (SELECT toUInt64(1) IN with_ids) AS present;

SELECT name, dependencies_table FROM system.tables
WHERE database = currentDatabase() AND name IN ('ids', 'with_ids') ORDER BY name;

RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.v TO {CLICKHOUSE_DATABASE_2:Identifier}.ids;
SELECT * FROM {CLICKHOUSE_DATABASE_2:Identifier}.ids;

DROP TABLE ids SETTINGS check_referential_table_dependencies = 1;
DROP TABLE with_ids SETTINGS check_referential_table_dependencies = 1;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
