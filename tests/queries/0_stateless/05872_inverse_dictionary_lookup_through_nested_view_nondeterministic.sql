-- Tags: no-parallel-replicas
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

-- `optimize_inverse_dictionary_lookup` inspects a view separately from the analysis done when the view is read.
-- A view that reads another view can have the body of the latter inlined (`analyzer_inline_views`) into the tree
-- resolved for inspection. A non-deterministic dictionary or attribute name in the nested view must still stop
-- the rewrite: each analysis folds `randConstant` on its own.

SET enable_analyzer = 1;

DROP VIEW IF EXISTS nested_wrapped_attr_v;
DROP VIEW IF EXISTS nested_wrapped_dict_v;
DROP VIEW IF EXISTS nested_nondeterministic_attr_v;
DROP VIEW IF EXISTS nested_nondeterministic_dict_v;
DROP DICTIONARY IF EXISTS nested_dict_1;
DROP DICTIONARY IF EXISTS nested_dict_2;
DROP TABLE IF EXISTS nested_ref_1;
DROP TABLE IF EXISTS nested_ref_2;
DROP TABLE IF EXISTS nested_data;

CREATE TABLE nested_ref_1 (id UInt64, a String, b String) ENGINE = MergeTree ORDER BY id;
INSERT INTO nested_ref_1 VALUES (1, 'match', 'other'), (2, 'other', 'match');

CREATE TABLE nested_ref_2 (id UInt64, a String, b String) ENGINE = MergeTree ORDER BY id;
INSERT INTO nested_ref_2 VALUES (1, 'other', 'match'), (2, 'match', 'other');

CREATE DICTIONARY nested_dict_1 (id UInt64, a String, b String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'nested_ref_1')) LAYOUT(HASHED()) LIFETIME(0);

CREATE DICTIONARY nested_dict_2 (id UInt64, a String, b String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'nested_ref_2')) LAYOUT(HASHED()) LIFETIME(0);

CREATE TABLE nested_data (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO nested_data VALUES (1), (2);

CREATE VIEW nested_nondeterministic_attr_v SQL SECURITY INVOKER AS
SELECT id, dictGetString('nested_dict_1', if(randConstant() % 2 = 0, 'a', 'b'), id) AS name
FROM nested_data;

CREATE VIEW nested_nondeterministic_dict_v SQL SECURITY INVOKER AS
SELECT id, dictGetString(if(randConstant() % 2 = 0, 'nested_dict_1', 'nested_dict_2'), 'a', id) AS name
FROM nested_data;

-- The outer views have no `dictGet` of their own, and inline the views they read.
CREATE VIEW nested_wrapped_attr_v SQL SECURITY INVOKER AS
SELECT id, name FROM nested_nondeterministic_attr_v SETTINGS analyzer_inline_views = 1;

CREATE VIEW nested_wrapped_dict_v SQL SECURITY INVOKER AS
SELECT id, name FROM nested_nondeterministic_dict_v SETTINGS analyzer_inline_views = 1;

SET analyzer_inline_views = 0, optimize_inverse_dictionary_lookup = 1;

SELECT 'the filter is left on the view column';
SELECT 'attribute', count() > 0 FROM (EXPLAIN QUERY TREE SELECT id FROM nested_wrapped_attr_v WHERE name = 'match') WHERE explain LIKE '%column_name: name,%';
SELECT 'dictionary', count() > 0 FROM (EXPLAIN QUERY TREE SELECT id FROM nested_wrapped_dict_v WHERE name = 'match') WHERE explain LIKE '%column_name: name,%';

-- Whichever attribute or dictionary is chosen, exactly one row matches, and its `name` is `match`.
SELECT 'attribute';
SELECT name FROM nested_wrapped_attr_v WHERE name = 'match';
SELECT name FROM nested_wrapped_attr_v WHERE name = 'match';
SELECT name FROM nested_wrapped_attr_v WHERE name = 'match';
SELECT name FROM nested_wrapped_attr_v WHERE name = 'match';

SELECT 'dictionary';
SELECT name FROM nested_wrapped_dict_v WHERE name = 'match';
SELECT name FROM nested_wrapped_dict_v WHERE name = 'match';
SELECT name FROM nested_wrapped_dict_v WHERE name = 'match';
SELECT name FROM nested_wrapped_dict_v WHERE name = 'match';

DROP VIEW nested_wrapped_attr_v;
DROP VIEW nested_wrapped_dict_v;
DROP VIEW nested_nondeterministic_attr_v;
DROP VIEW nested_nondeterministic_dict_v;
DROP DICTIONARY nested_dict_1;
DROP DICTIONARY nested_dict_2;
DROP TABLE nested_ref_1;
DROP TABLE nested_ref_2;
DROP TABLE nested_data;
