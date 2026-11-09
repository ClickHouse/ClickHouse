-- An `ABR` table keeps the IDs of its source tables with the name of its database, by which they would no longer be found
-- after `RENAME DATABASE`. Renaming a database that contains an `ABR` table is rejected before anything is renamed, also
-- without the check of the dependencies, and it can be renamed after the `ABR` table is dropped.

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

SELECT 'rename the database';
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError NOT_IMPLEMENTED }
SET check_table_dependencies = 0;
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier}; -- { serverError NOT_IMPLEMENTED }
SET check_table_dependencies = 1;
SELECT count() FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_2:String};
SELECT max(value) FROM {CLICKHOUSE_DATABASE_1:Identifier}.abr WHERE ts >= toDateTime('2026-01-01 00:00:00');

SELECT 'rename the database without the ABR table';
DROP TABLE {CLICKHOUSE_DATABASE_1:Identifier}.abr;
RENAME DATABASE {CLICKHOUSE_DATABASE_1:Identifier} TO {CLICKHOUSE_DATABASE_2:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.abr ENGINE = ABR('src', [1, 10], ('ts'));
SELECT max(value) FROM {CLICKHOUSE_DATABASE_2:Identifier}.abr WHERE ts >= toDateTime('2026-01-01 00:00:00');

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
