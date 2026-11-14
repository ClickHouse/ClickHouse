-- Tags: no-parallel-replicas
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

-- `optimize_inverse_dictionary_lookup` must not look through a view column whose `dictGet` depends on a
-- scalar subquery: the view is inspected without executing it, and the placeholder value of the subquery
-- can select another dictionary or attribute than the one the view reads.

SET enable_analyzer = 1;

DROP VIEW IF EXISTS scalar_subquery_attr_v;
DROP VIEW IF EXISTS scalar_subquery_dict_v;
DROP DICTIONARY IF EXISTS scalar_subquery_dict;
DROP TABLE IF EXISTS scalar_subquery_ref;
DROP TABLE IF EXISTS scalar_subquery_data;
DROP TABLE IF EXISTS scalar_subquery_cfg;

CREATE TABLE scalar_subquery_cfg (flag UInt8, dict_name String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO scalar_subquery_cfg SELECT 1, currentDatabase() || '.scalar_subquery_dict';

CREATE TABLE scalar_subquery_ref (id UInt64, a String, b String) ENGINE = MergeTree ORDER BY id;
INSERT INTO scalar_subquery_ref VALUES (1, 'match', 'other'), (2, 'other', 'match');

CREATE DICTIONARY scalar_subquery_dict (id UInt64, a String, b String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'scalar_subquery_ref')) LAYOUT(HASHED()) LIFETIME(0);

CREATE TABLE scalar_subquery_data (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO scalar_subquery_data VALUES (1), (2);

CREATE VIEW scalar_subquery_attr_v (id UInt64, name String) SQL SECURITY INVOKER AS
SELECT id, dictGetString('scalar_subquery_dict', if(assumeNotNull((SELECT flag FROM scalar_subquery_cfg LIMIT 1)) = 0, 'a', 'b'), id) AS name
FROM scalar_subquery_data;

CREATE VIEW scalar_subquery_dict_v (id UInt64, name String) SQL SECURITY INVOKER AS
SELECT id, dictGetString((SELECT dict_name FROM scalar_subquery_cfg LIMIT 1), 'b', id) AS name
FROM scalar_subquery_data;

SELECT 'attribute selected by a scalar subquery';
SELECT id FROM scalar_subquery_attr_v WHERE name = 'match' ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT id FROM scalar_subquery_attr_v WHERE name = 'match' ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT 'dictionary selected by a scalar subquery';
SELECT id FROM scalar_subquery_dict_v WHERE name = 'match' ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT id FROM scalar_subquery_dict_v WHERE name = 'match' ORDER BY id SETTINGS optimize_inverse_dictionary_lookup = 1;

DROP VIEW scalar_subquery_attr_v;
DROP VIEW scalar_subquery_dict_v;
DROP DICTIONARY scalar_subquery_dict;
DROP TABLE scalar_subquery_ref;
DROP TABLE scalar_subquery_data;
DROP TABLE scalar_subquery_cfg;
