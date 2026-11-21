-- Tags: no-parallel-replicas
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

-- `optimize_inverse_dictionary_lookup` inspects a view separately from the analysis done when the view is read.
-- A constant folded from a non-deterministic function (`randConstant`) has its own value in each of them,
-- so a dictionary or attribute name selected by it must not be used for the rewrite: the rewritten filter
-- could select the keys by another attribute than the one the view projects.

SET enable_analyzer = 1;

DROP VIEW IF EXISTS nondeterministic_attr_v;
DROP VIEW IF EXISTS nondeterministic_dict_v;
DROP VIEW IF EXISTS deterministic_attr_v;
DROP DICTIONARY IF EXISTS nondeterministic_dict_1;
DROP DICTIONARY IF EXISTS nondeterministic_dict_2;
DROP TABLE IF EXISTS nondeterministic_ref_1;
DROP TABLE IF EXISTS nondeterministic_ref_2;
DROP TABLE IF EXISTS nondeterministic_data;

CREATE TABLE nondeterministic_ref_1 (id UInt64, a String, b String) ENGINE = MergeTree ORDER BY id;
INSERT INTO nondeterministic_ref_1 VALUES (1, 'match', 'other'), (2, 'other', 'match');

CREATE TABLE nondeterministic_ref_2 (id UInt64, a String, b String) ENGINE = MergeTree ORDER BY id;
INSERT INTO nondeterministic_ref_2 VALUES (1, 'other', 'match'), (2, 'match', 'other');

CREATE DICTIONARY nondeterministic_dict_1 (id UInt64, a String, b String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'nondeterministic_ref_1')) LAYOUT(HASHED()) LIFETIME(0);

CREATE DICTIONARY nondeterministic_dict_2 (id UInt64, a String, b String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'nondeterministic_ref_2')) LAYOUT(HASHED()) LIFETIME(0);

CREATE TABLE nondeterministic_data (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO nondeterministic_data VALUES (1), (2);

CREATE VIEW nondeterministic_attr_v SQL SECURITY INVOKER AS
SELECT id, dictGetString('nondeterministic_dict_1', if(randConstant() % 2 = 0, 'a', 'b'), id) AS name
FROM nondeterministic_data;

CREATE VIEW nondeterministic_dict_v SQL SECURITY INVOKER AS
SELECT id, dictGetString(if(randConstant() % 2 = 0, 'nondeterministic_dict_1', 'nondeterministic_dict_2'), 'a', id) AS name
FROM nondeterministic_data;

-- The same shape with literal names, to check that the rewrite is still applied to it.
CREATE VIEW deterministic_attr_v SQL SECURITY INVOKER AS
SELECT id, dictGetString('nondeterministic_dict_1', 'b', id) AS name
FROM nondeterministic_data;

SET optimize_inverse_dictionary_lookup = 1;

SELECT 'the filter is left on the view column';
SELECT 'deterministic attribute', count() > 0 FROM (EXPLAIN QUERY TREE SELECT id FROM deterministic_attr_v WHERE name = 'match') WHERE explain LIKE '%column_name: name,%';
SELECT 'non-deterministic attribute', count() > 0 FROM (EXPLAIN QUERY TREE SELECT id FROM nondeterministic_attr_v WHERE name = 'match') WHERE explain LIKE '%column_name: name,%';
SELECT 'non-deterministic dictionary', count() > 0 FROM (EXPLAIN QUERY TREE SELECT id FROM nondeterministic_dict_v WHERE name = 'match') WHERE explain LIKE '%column_name: name,%';

SELECT 'deterministic attribute';
SELECT id, name FROM deterministic_attr_v WHERE name = 'match';

-- Whichever attribute or dictionary is chosen, exactly one row matches, and its `name` is `match`.
SELECT 'non-deterministic attribute';
SELECT name FROM nondeterministic_attr_v WHERE name = 'match';
SELECT name FROM nondeterministic_attr_v WHERE name = 'match';
SELECT name FROM nondeterministic_attr_v WHERE name = 'match';
SELECT name FROM nondeterministic_attr_v WHERE name = 'match';
SELECT name FROM nondeterministic_attr_v WHERE name = 'match';
SELECT name FROM nondeterministic_attr_v WHERE name = 'match';
SELECT name FROM nondeterministic_attr_v WHERE name = 'match';
SELECT name FROM nondeterministic_attr_v WHERE name = 'match';

SELECT 'non-deterministic dictionary';
SELECT name FROM nondeterministic_dict_v WHERE name = 'match';
SELECT name FROM nondeterministic_dict_v WHERE name = 'match';
SELECT name FROM nondeterministic_dict_v WHERE name = 'match';
SELECT name FROM nondeterministic_dict_v WHERE name = 'match';
SELECT name FROM nondeterministic_dict_v WHERE name = 'match';
SELECT name FROM nondeterministic_dict_v WHERE name = 'match';
SELECT name FROM nondeterministic_dict_v WHERE name = 'match';
SELECT name FROM nondeterministic_dict_v WHERE name = 'match';

DROP VIEW nondeterministic_attr_v;
DROP VIEW nondeterministic_dict_v;
DROP VIEW deterministic_attr_v;
DROP DICTIONARY nondeterministic_dict_1;
DROP DICTIONARY nondeterministic_dict_2;
DROP TABLE nondeterministic_ref_1;
DROP TABLE nondeterministic_ref_2;
DROP TABLE nondeterministic_data;
