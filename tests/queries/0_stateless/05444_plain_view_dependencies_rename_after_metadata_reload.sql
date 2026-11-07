-- Regression test: moving a reloaded ordinary view to another database must keep its dependencies on the
-- sources the loaded view reads. The stored definition keeps the source unqualified, but loading it
-- qualifies the source with the database of the view, so the moved view keeps reading the old source.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Atomic;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (id UInt64) ENGINE = Memory;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.src (id UInt64) ENGINE = Memory;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (1);
INSERT INTO {CLICKHOUSE_DATABASE_2:Identifier}.src VALUES (2);

USE {CLICKHOUSE_DATABASE_1:Identifier};
-- The temporary table shadowing the source is what keeps the stored definition of the view unqualified:
-- `AddDefaultDatabaseVisitor` does not qualify names of session-local external tables.
CREATE TEMPORARY TABLE src (id UInt64) ENGINE = Memory;
CREATE VIEW v AS SELECT * FROM src;
DROP TEMPORARY TABLE src;

-- Guard the precondition: the source of the view is stored unqualified.
SELECT 'source is stored unqualified', position(create_table_query, '.src') = 0
FROM system.tables WHERE database = currentDatabase() AND name = 'v';
USE {CLICKHOUSE_DATABASE:Identifier};

DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- The data of a `Memory` table does not survive the reload.
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (1);

RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.v TO {CLICKHOUSE_DATABASE_2:Identifier}.v;

SELECT 'view reads', * FROM {CLICKHOUSE_DATABASE_2:Identifier}.v;

SELECT if(database = {CLICKHOUSE_DATABASE_1:String}, 'db1', 'db2') AS db, dependencies_table
FROM system.tables WHERE name = 'src' AND database IN ({CLICKHOUSE_DATABASE_1:String}, {CLICKHOUSE_DATABASE_2:String}) ORDER BY db;

DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src SETTINGS check_table_dependencies = 1, check_referential_table_dependencies = 1; -- { serverError HAVE_DEPENDENT_OBJECTS }
DROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.src SETTINGS check_table_dependencies = 1, check_referential_table_dependencies = 1;

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
