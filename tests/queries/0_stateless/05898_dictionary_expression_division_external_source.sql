-- Tags: no-fasttest
-- Tag no-fasttest: the `MYSQL` and `POSTGRESQL` dictionary sources are not in the fast test build

-- With `sql_compatible_division_for_integers = 1`, a `/` in a new query is stored as `divideExact`.
-- The `EXPRESSION` of a dictionary attribute is not evaluated by this server: it is pasted into the query
-- that the source of the dictionary runs. `MySQL` and `PostgreSQL` have no `divideExact`, so a dictionary
-- over them that was created with the setting never loaded. The expression is now left as it was written
-- unless the source is `CLICKHOUSE`.
-- See https://github.com/ClickHouse/ClickHouse/issues/125520

DROP DICTIONARY IF EXISTS d_mysql;
DROP DICTIONARY IF EXISTS d_postgresql;
DROP DICTIONARY IF EXISTS d_clickhouse;
DROP DICTIONARY IF EXISTS d_clickhouse_legacy;
DROP TABLE IF EXISTS src;

CREATE TABLE src (id UInt64, v Int64) ENGINE = Memory;
INSERT INTO src VALUES (1, 7);

-- The dictionaries over `MySQL` and `PostgreSQL` are only created, never loaded: nothing listens at the address.
SET sql_compatible_division_for_integers = 1;

CREATE DICTIONARY d_mysql (id UInt64, half Float64 EXPRESSION v / 2)
PRIMARY KEY id SOURCE(MYSQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD '' DB 'd' TABLE 't')) LAYOUT(FLAT()) LIFETIME(0);

CREATE DICTIONARY d_postgresql (id UInt64, half Float64 EXPRESSION v / 2)
PRIMARY KEY id SOURCE(POSTGRESQL(HOST '127.0.0.1' PORT 1 USER 'u' PASSWORD '' DB 'd' TABLE 't')) LAYOUT(FLAT()) LIFETIME(0);

CREATE DICTIONARY d_clickhouse (id UInt64, half Float64 EXPRESSION v / 2)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(0);

SET sql_compatible_division_for_integers = 0;

CREATE DICTIONARY d_clickhouse_legacy (id UInt64, half Float64 EXPRESSION v / 2)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src' DB currentDatabase())) LAYOUT(FLAT()) LIFETIME(0);

SELECT '-- the stored expression: as it was written for an external source, exact division for ClickHouse';
SELECT
    name,
    position(create_table_query, 'EXPRESSION v / 2') > 0 AS as_written,
    position(create_table_query, 'divideExact') > 0 AS exact_division
FROM system.tables
WHERE database = currentDatabase() AND name LIKE 'd\\_%'
ORDER BY name;

SELECT '-- a ClickHouse source still divides as the setting said when the dictionary was created';
SELECT dictGet('d_clickhouse', 'half', toUInt64(1)), dictGet('d_clickhouse_legacy', 'half', toUInt64(1));

DROP DICTIONARY d_mysql;
DROP DICTIONARY d_postgresql;
DROP DICTIONARY d_clickhouse;
DROP DICTIONARY d_clickhouse_legacy;
DROP TABLE src;
