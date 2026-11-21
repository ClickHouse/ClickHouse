-- Tags: no-parallel-replicas
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

-- `optimize_inverse_dictionary_lookup` must not look through a view column where the column does not
-- hold the result of its `dictGet` or where the key column is not available.

SET enable_analyzer = 1;

DROP VIEW IF EXISTS row_semantics_v;
DROP VIEW IF EXISTS row_semantics_fill_v;
DROP DICTIONARY IF EXISTS row_semantics_dict;
DROP TABLE IF EXISTS row_semantics_ref;
DROP TABLE IF EXISTS row_semantics_data;

CREATE TABLE row_semantics_ref (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO row_semantics_ref VALUES (0, 'match'), (1, 'other');

CREATE DICTIONARY row_semantics_dict (id UInt64, name String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'row_semantics_ref')) LAYOUT(HASHED()) LIFETIME(0);

CREATE TABLE row_semantics_data (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO row_semantics_data VALUES (1);

CREATE VIEW row_semantics_v SQL SECURITY INVOKER AS
SELECT id, dictGetString('row_semantics_dict', 'name', id) AS name FROM row_semantics_data;

CREATE VIEW row_semantics_fill_v SQL SECURITY INVOKER AS
SELECT id, dictGetString('row_semantics_dict', 'name', id) AS name FROM row_semantics_data
ORDER BY id WITH FILL FROM 0 TO 3;

-- An unmatched row of the right side of a `LEFT JOIN` has the default `name`, not `dictGet` of the default `id`.
SELECT 'left join';
SELECT count()
FROM (SELECT toUInt64(2) AS id) AS l LEFT JOIN row_semantics_v AS v ON l.id = v.id
WHERE v.name = 'match'
SETTINGS join_use_nulls = 0, optimize_inverse_dictionary_lookup = 0;

SELECT count()
FROM (SELECT toUInt64(2) AS id) AS l LEFT JOIN row_semantics_v AS v ON l.id = v.id
WHERE v.name = 'match'
SETTINGS join_use_nulls = 0, optimize_inverse_dictionary_lookup = 1;

-- `WITH FILL` generates rows with the default `name`.
SELECT 'with fill';
SELECT count() FROM row_semantics_fill_v WHERE name = 'match' SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT count() FROM row_semantics_fill_v WHERE name = 'match' SETTINGS optimize_inverse_dictionary_lookup = 1;

-- After aggregation by `name` the key column `id` is not available.
SELECT 'group by';
SELECT name, name = 'other', count() FROM row_semantics_v GROUP BY name SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT name, name = 'other', count() FROM row_semantics_v GROUP BY name SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT name, count() FROM row_semantics_v GROUP BY name HAVING name = 'other' SETTINGS optimize_inverse_dictionary_lookup = 1;

-- The rewrite still applies in `WHERE` of the query reading the view and on the preserved side of a join.
SELECT 'rewritten';
SELECT count() FROM row_semantics_v WHERE name = 'other' SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM row_semantics_v WHERE name = 'other' SETTINGS optimize_inverse_dictionary_lookup = 1)
WHERE explain LIKE '%column_name: id,%';
SELECT count()
FROM row_semantics_v AS v INNER JOIN (SELECT toUInt64(1) AS id) AS r ON v.id = r.id
WHERE v.name = 'other'
SETTINGS optimize_inverse_dictionary_lookup = 1;

DROP VIEW row_semantics_fill_v;
DROP VIEW row_semantics_v;
DROP DICTIONARY row_semantics_dict;
DROP TABLE row_semantics_data;
DROP TABLE row_semantics_ref;
