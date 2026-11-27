-- Reading a dictionary as a table calls `IDictionary::read` when the pipeline is built, not when the query plan is,
-- so a JOIN that looks the keys up in the dictionary does not pay for it.
-- https://github.com/ClickHouse/ClickHouse/issues/129777

SET enable_analyzer = 1;
SET join_use_nulls = 0;

DROP DICTIONARY IF EXISTS dict_hashed;
DROP DICTIONARY IF EXISTS dict_direct;
DROP DICTIONARY IF EXISTS dict_polygon;
DROP TABLE IF EXISTS dim;
DROP TABLE IF EXISTS fact;
DROP TABLE IF EXISTS polygons;

CREATE TABLE dim (id UInt64, name String, value UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO dim SELECT number, 'name_' || toString(number), number * 2 FROM numbers(1000);

-- The keys 0, 150, ..., 900 are in the dictionaries, 1050, 1200 and 1350 are not.
CREATE TABLE fact (id UInt64, key UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO fact SELECT number, number * 150 FROM numbers(10);

CREATE DICTIONARY dict_hashed (id UInt64, name String, value UInt32)
PRIMARY KEY id SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'dim')) LAYOUT(HASHED()) LIFETIME(0);

CREATE DICTIONARY dict_direct (id UInt64, name String, value UInt32)
PRIMARY KEY id SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'dim')) LAYOUT(DIRECT());

-- The plans are checked without parallel replicas: those copy the dictionary into a temporary table for a `GLOBAL` JOIN,
-- so the plan has neither a JOIN that looks the keys up nor the step that reads the dictionary.
SELECT 'A JOIN that looks the keys up has no reading step';
SELECT count() FROM (EXPLAIN actions = 1 SELECT f.id, d.name FROM fact AS f LEFT JOIN dict_hashed AS d ON f.key = d.id SETTINGS enable_parallel_replicas = 0)
WHERE explain LIKE '%Algorithm: DirectKeyValueJoin%';
SELECT count() FROM (EXPLAIN SELECT f.id, d.name FROM fact AS f LEFT JOIN dict_hashed AS d ON f.key = d.id SETTINGS enable_parallel_replicas = 0)
WHERE explain LIKE '%ReadFromDictionary%';
SELECT f.id, d.name FROM fact AS f LEFT JOIN dict_hashed AS d ON f.key = d.id ORDER BY f.id;

SELECT 'A hash JOIN reads the dictionary';
SELECT count() FROM (EXPLAIN SELECT f.id, d.name FROM fact AS f LEFT JOIN dict_hashed AS d ON f.key = d.id SETTINGS join_algorithm = 'hash', enable_parallel_replicas = 0)
WHERE explain LIKE '%ReadFromDictionary%';
SELECT f.id, d.name FROM fact AS f LEFT JOIN dict_hashed AS d ON f.key = d.id ORDER BY f.id SETTINGS join_algorithm = 'hash';

SELECT 'Reading the dictionary as a table';
SELECT count(), sum(id), sum(value) FROM dict_hashed;
SELECT name, id FROM dict_hashed WHERE id IN (3, 500, 999) ORDER BY id;
SELECT _table, count() FROM dict_hashed GROUP BY _table;
SELECT count() FROM dict_hashed SETTINGS max_rows_to_read = 10; -- { serverError TOO_MANY_ROWS }

SELECT 'A layout that returns all the columns of its source';
SELECT name FROM dict_direct WHERE id IN (3, 500, 999) ORDER BY name;
SELECT value, id FROM dict_direct WHERE id IN (3, 500, 999) ORDER BY id;
SELECT count(), sum(value) FROM dict_direct;
SELECT f.id, d.name FROM fact AS f LEFT JOIN dict_direct AS d ON f.key = d.id ORDER BY f.id SETTINGS join_algorithm = 'hash';

SELECT 'A dictionary that cannot be read fails when the pipeline is built';
CREATE TABLE polygons (key Array(Array(Array(Tuple(Float64, Float64)))), name String) ENGINE = MergeTree ORDER BY name;
INSERT INTO polygons VALUES ([[[(3, 1), (0, 1), (0, -1), (3, -1)]]], 'Value');

CREATE DICTIONARY dict_polygon (key Array(Array(Array(Tuple(Float64, Float64)))), name String)
PRIMARY KEY key SOURCE(CLICKHOUSE(DB currentDatabase() TABLE 'polygons')) LAYOUT(POLYGON()) LIFETIME(0);

SELECT count() FROM (EXPLAIN SELECT name FROM dict_polygon) WHERE explain LIKE '%ReadFromDictionary%';
EXPLAIN PIPELINE SELECT name FROM dict_polygon; -- { serverError UNSUPPORTED_METHOD }
SELECT name FROM dict_polygon; -- { serverError UNSUPPORTED_METHOD }

DROP DICTIONARY dict_polygon;
DROP DICTIONARY dict_direct;
DROP DICTIONARY dict_hashed;
DROP TABLE polygons;
DROP TABLE fact;
DROP TABLE dim;
