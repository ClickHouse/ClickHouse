-- An `ABR` table finds its source tables in its own database on every query, so they are still found after `RENAME DATABASE`.
-- As for any database with dependencies between its tables, the rename is refused while `check_table_dependencies` is enabled.

SET allow_experimental_abr = 1;
SET session_timezone = 'UTC';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src_10 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src_1 VALUES ('2026-01-01 00:00:00', 1);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src_10 VALUES ('2026-01-01 00:00:00', 10);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.abr ENGINE = ABR('src', [1, 10], ('ts'));

RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError HAVE_DEPENDENT_OBJECTS }
SET check_table_dependencies = 0;
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier};
SET check_table_dependencies = 1;

SELECT max(value) FROM {CLICKHOUSE_DATABASE_2:Identifier}.abr WHERE ts >= toDateTime('2026-01-01 00:00:00');

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
