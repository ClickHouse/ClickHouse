-- Regression test: exchanging a view with its source table in another database must be rejected,
-- because the view would then select from itself.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE IF EXISTS src;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;

CREATE TABLE src (id UInt64) ENGINE = Memory;
INSERT INTO src VALUES (1);
CREATE VIEW {CLICKHOUSE_DATABASE_1:Identifier}.v AS SELECT * FROM src;

EXCHANGE TABLES {CLICKHOUSE_DATABASE_1:Identifier}.v AND src; -- { serverError INFINITE_LOOP }
EXCHANGE TABLES src AND {CLICKHOUSE_DATABASE_1:Identifier}.v; -- { serverError INFINITE_LOOP }

SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.v;
SELECT * FROM src;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE src;
