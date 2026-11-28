-- The trailing `SETTINGS` clause of a set operation stays query-level when it is nested into `INTERSECT`,
-- including when the query tree is sent to a remote server, and when a mutation expands the common
-- table expressions and qualifies the table names of its subqueries.

SET transform_null_in = 0;

-- The intersection is empty, so every result comes from the first arm, which sees the clause.
SELECT 'union, local', x FROM (SELECT getSetting('transform_null_in') AS x UNION ALL SELECT false INTERSECT ALL SELECT true SETTINGS transform_null_in = 1);
SELECT 'union, remote', x FROM remote('127.0.0.1', view(SELECT getSetting('transform_null_in') AS x UNION ALL SELECT false INTERSECT ALL SELECT true SETTINGS transform_null_in = 1));
SELECT 'except, local', x FROM (SELECT getSetting('transform_null_in') AS x EXCEPT ALL SELECT false INTERSECT ALL SELECT true SETTINGS transform_null_in = 1);
SELECT 'except, remote', x FROM remote('127.0.0.1', view(SELECT getSetting('transform_null_in') AS x EXCEPT ALL SELECT false INTERSECT ALL SELECT true SETTINGS transform_null_in = 1));
-- Parentheses keep the clause to the `INTERSECT`.
SELECT 'parenthesized, local', x FROM (SELECT getSetting('transform_null_in') AS x UNION ALL (SELECT false INTERSECT ALL SELECT true SETTINGS transform_null_in = 1));
SELECT 'parenthesized, remote', x FROM remote('127.0.0.1', view(SELECT getSetting('transform_null_in') AS x UNION ALL (SELECT false INTERSECT ALL SELECT true SETTINGS transform_null_in = 1)));

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS u;
CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (2);
CREATE TABLE u (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO u VALUES (1, 0), (2, 0);

-- `enable_global_with_statement = 0` applies to the first arm too, so it reads the table, not the common table expression.
SELECT (WITH src AS (SELECT 7 AS id) SELECT sum(m) FROM (SELECT max(id) * 1000 AS m FROM src UNION ALL SELECT max(id) AS m FROM src INTERSECT ALL SELECT toUInt64(2) AS m SETTINGS enable_global_with_statement = 0));
UPDATE u SET v = (WITH src AS (SELECT 7 AS id) SELECT sum(m) FROM (SELECT max(id) * 1000 AS m FROM src UNION ALL SELECT max(id) AS m FROM src INTERSECT ALL SELECT toUInt64(2) AS m SETTINGS enable_global_with_statement = 0)) WHERE id = 1;
ALTER TABLE u UPDATE v = (WITH src AS (SELECT 7 AS id) SELECT sum(m) FROM (SELECT max(id) * 1000 AS m FROM src UNION ALL SELECT max(id) AS m FROM src INTERSECT ALL SELECT toUInt64(2) AS m SETTINGS enable_global_with_statement = 0)) WHERE id = 2 SETTINGS mutations_sync = 2;
SELECT * FROM u ORDER BY id;

-- A parenthesized operand with its own clause is qualified with the database of the mutated table.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.src VALUES (3);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.u (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.u VALUES (1, 0), (2, 0);
UPDATE {CLICKHOUSE_DATABASE_1:Identifier}.u SET v = (SELECT sum(m) FROM (SELECT toUInt64(0) AS m UNION ALL (SELECT max(id) AS m FROM src SETTINGS max_threads = 1))) WHERE id = 1;
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.u UPDATE v = (SELECT sum(m) FROM (SELECT toUInt64(0) AS m UNION ALL (SELECT max(id) AS m FROM src SETTINGS max_threads = 1))) WHERE id = 2 SETTINGS mutations_sync = 2;
SELECT * FROM {CLICKHOUSE_DATABASE_1:Identifier}.u ORDER BY id;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE src;
DROP TABLE u;
