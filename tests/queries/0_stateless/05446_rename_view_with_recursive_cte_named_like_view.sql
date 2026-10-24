-- Regression test: a reference of a recursive common table expression to itself is not a table, even when it is named
-- like the view or like an existing table. A view reading such a common table expression is moved to another database
-- by `RENAME TABLE` and by `RENAME DATABASE`, and it is not a dependent of the table with the same name.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Atomic;

CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v AS
    WITH RECURSIVE v AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM v WHERE n < 3)
    SELECT * FROM v;

SELECT '-- RENAME TABLE';
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.v TO {CLICKHOUSE_DATABASE_2:Identifier}.v;
SELECT * FROM {CLICKHOUSE_DATABASE_2:Identifier}.v ORDER BY n;

SELECT '-- RENAME DATABASE';
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
RENAME DATABASE {CLICKHOUSE_DATABASE_2:Identifier} TO {CLICKHOUSE_DATABASE_1:Identifier};
SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.v ORDER BY n;

SELECT '-- a table named like the common table expression';
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.r (n UInt64) ENGINE = Memory;
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.reads_cte AS
    WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r WHERE n < 3)
    SELECT * FROM r;
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT name, dependencies_table FROM system.tables WHERE database = currentDatabase() AND name = 'r';
USE {CLICKHOUSE_DATABASE:Identifier};

SELECT '-- a view reading a table of the destination database named like it';
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Atomic;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.w (n UInt64) ENGINE = Memory;
-- The source is qualified with the current database when the view is created.
USE {CLICKHOUSE_DATABASE_2:Identifier};
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.w AS SELECT * FROM w;
USE {CLICKHOUSE_DATABASE:Identifier};
SET check_referential_table_dependencies = 0;
DROP TABLE {CLICKHOUSE_DATABASE_2:Identifier}.w SYNC;
RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.w TO {CLICKHOUSE_DATABASE_2:Identifier}.w; -- { serverError INFINITE_LOOP }

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
