-- A failed `CREATE` of a `TimeSeries` table drops only the inner tables it created. In an `Ordinary` database the name
-- of an inner table does not depend on a UUID, so a table with that name may exist already: it must keep its rows.

SET allow_experimental_time_series_table = 1;
SET allow_deprecated_database_ordinary = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Ordinary;

-- The `tags` inner table is created after the `samples` one: the `CREATE` creates `samples`, then fails on `tags`.
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.`.inner.tags.ts` (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.`.inner.tags.ts` VALUES (42);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts ENGINE = TimeSeries; -- { serverError TABLE_ALREADY_EXISTS }

-- The existing table keeps its rows, and the `samples` inner table that the failed `CREATE` created is dropped.
SELECT x FROM {CLICKHOUSE_DATABASE_1:Identifier}.`.inner.tags.ts`;
-- `CLICKHOUSE_DATABASE_1` is the name of the test database with `_1` appended.
SELECT name FROM system.tables WHERE database = currentDatabase() || '_1' ORDER BY name;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
