-- Tags: no-parallel-replicas
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

-- `optimize_inverse_dictionary_lookup` looks through a view column only where the view's rows are preserved by
-- the enclosing query. Every query below must return the same rows with the setting off and on. The dictionary
-- maps the key `0` to `match`, so a rewrite on the extended side of an outer join would wrongly select the
-- unmatched rows, where the view column is `''` and the key column is `0` (`join_use_nulls = 0`).

SET enable_analyzer = 1;
SET join_use_nulls = 0;

DROP VIEW IF EXISTS join_scope_v;
DROP DICTIONARY IF EXISTS join_scope_dict;
DROP TABLE IF EXISTS join_scope_ref;
DROP TABLE IF EXISTS join_scope_data;
DROP TABLE IF EXISTS join_scope_other;

CREATE TABLE join_scope_ref (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO join_scope_ref VALUES (0, 'match'), (1, 'match'), (2, 'other');

CREATE DICTIONARY join_scope_dict (id UInt64, name String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'join_scope_ref')) LAYOUT(HASHED()) LIFETIME(0);

CREATE TABLE join_scope_data (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO join_scope_data VALUES (1), (2);

CREATE TABLE join_scope_other (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO join_scope_other VALUES (1), (3);

CREATE VIEW join_scope_v SQL SECURITY INVOKER AS
SELECT id, dictGetString('join_scope_dict', 'name', id) AS name FROM join_scope_data;

SELECT 'right join, view on the extended side';
SELECT o.id, v.id, v.name FROM join_scope_v AS v RIGHT JOIN join_scope_other AS o ON v.id = o.id WHERE v.name = 'match' ORDER BY o.id SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT o.id, v.id, v.name FROM join_scope_v AS v RIGHT JOIN join_scope_other AS o ON v.id = o.id WHERE v.name = 'match' ORDER BY o.id SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'full join, both sides extended';
SELECT o.id, v.id, v.name FROM join_scope_v AS v FULL JOIN join_scope_other AS o ON v.id = o.id WHERE v.name = 'match' ORDER BY o.id, v.id SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT o.id, v.id, v.name FROM join_scope_v AS v FULL JOIN join_scope_other AS o ON v.id = o.id WHERE v.name = 'match' ORDER BY o.id, v.id SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'left join, view on the preserved side';
SELECT v.id, o.id FROM join_scope_v AS v LEFT JOIN join_scope_other AS o ON v.id = o.id WHERE v.name = 'match' ORDER BY v.id SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT v.id, o.id FROM join_scope_v AS v LEFT JOIN join_scope_other AS o ON v.id = o.id WHERE v.name = 'match' ORDER BY v.id SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'array join';
SELECT v.id, x FROM join_scope_v AS v ARRAY JOIN [1, 2] AS x WHERE v.name = 'match' ORDER BY v.id, x SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT v.id, x FROM join_scope_v AS v ARRAY JOIN [1, 2] AS x WHERE v.name = 'match' ORDER BY v.id, x SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'or of several comparisons on the same view';
SELECT id FROM join_scope_v WHERE name = 'match' OR name = 'other' ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT id FROM join_scope_v WHERE name = 'match' OR name = 'other' ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 1;

DROP VIEW join_scope_v;
DROP DICTIONARY join_scope_dict;
DROP TABLE join_scope_ref;
DROP TABLE join_scope_data;
DROP TABLE join_scope_other;
