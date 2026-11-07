-- Regression test: moving an `Alias` with a target written without a database (`Alias('t')`) to another database
-- under the name of its own target must be rejected: the target follows the alias, so it would refer to itself.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Atomic;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (id UInt64) ENGINE = Memory;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (1);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.a ENGINE = Alias('src');

RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.a TO {CLICKHOUSE_DATABASE_2:Identifier}.src; -- { serverError INFINITE_LOOP }

CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.src (id UInt64) ENGINE = Memory;
EXCHANGE TABLES {CLICKHOUSE_DATABASE_1:Identifier}.a AND {CLICKHOUSE_DATABASE_2:Identifier}.src; -- { serverError INFINITE_LOOP }
EXCHANGE TABLES {CLICKHOUSE_DATABASE_2:Identifier}.src AND {CLICKHOUSE_DATABASE_1:Identifier}.a; -- { serverError INFINITE_LOOP }

SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.a;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
