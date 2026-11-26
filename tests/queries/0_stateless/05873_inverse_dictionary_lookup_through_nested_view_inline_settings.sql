-- Tags: no-parallel-replicas
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

-- `optimize_inverse_dictionary_lookup` must not use a non-deterministic dictionary or attribute name (`randConstant`)
-- from a view that is read by another view, whichever way the nested view is reached: with `analyzer_inline_views`
-- set by the session, and with a nested view in a subquery or in a `UNION ALL` of the inspected view.

SET enable_analyzer = 1;

DROP VIEW IF EXISTS inline_subquery_v;
DROP VIEW IF EXISTS inline_union_v;
DROP VIEW IF EXISTS inline_plain_v;
DROP VIEW IF EXISTS inline_nondeterministic_v;
DROP DICTIONARY IF EXISTS inline_dict;
DROP TABLE IF EXISTS inline_ref;
DROP TABLE IF EXISTS inline_data;

CREATE TABLE inline_ref (id UInt64, a String, b String) ENGINE = MergeTree ORDER BY id;
INSERT INTO inline_ref VALUES (1, 'match', 'other'), (2, 'other', 'match');

CREATE DICTIONARY inline_dict (id UInt64, a String, b String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'inline_ref')) LAYOUT(HASHED()) LIFETIME(0);

CREATE TABLE inline_data (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO inline_data VALUES (1), (2);

CREATE VIEW inline_nondeterministic_v SQL SECURITY INVOKER AS
SELECT id, dictGetString('inline_dict', if(randConstant() % 2 = 0, 'a', 'b'), id) AS name
FROM inline_data;

-- No `SETTINGS` in the definition: it is the session setting that inlines the nested view.
CREATE VIEW inline_plain_v SQL SECURITY INVOKER AS
SELECT id, name FROM inline_nondeterministic_v;

CREATE VIEW inline_subquery_v SQL SECURITY INVOKER AS
SELECT id, name FROM (SELECT id, name FROM inline_nondeterministic_v) SETTINGS analyzer_inline_views = 1;

CREATE VIEW inline_union_v SQL SECURITY INVOKER AS
SELECT id, name FROM (SELECT id, name FROM inline_nondeterministic_v UNION ALL SELECT id, name FROM inline_nondeterministic_v) SETTINGS analyzer_inline_views = 1;

SET optimize_inverse_dictionary_lookup = 1;

-- Whichever attribute is chosen, exactly one row of a view matches, and its `name` is `match`.
SELECT 'session setting';
SET analyzer_inline_views = 1;
SELECT name FROM inline_plain_v WHERE name = 'match';
SELECT name FROM inline_plain_v WHERE name = 'match';
SELECT name FROM inline_plain_v WHERE name = 'match';
SELECT name FROM inline_plain_v WHERE name = 'match';

SET analyzer_inline_views = 0;

SELECT 'nested view in a subquery, the filter is left on the view column';
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT id FROM inline_subquery_v WHERE name = 'match') WHERE explain LIKE '%column_name: name,%';
SELECT name FROM inline_subquery_v WHERE name = 'match';
SELECT name FROM inline_subquery_v WHERE name = 'match';
SELECT name FROM inline_subquery_v WHERE name = 'match';
SELECT name FROM inline_subquery_v WHERE name = 'match';

-- Each of the two branches has exactly one matching row.
SELECT 'nested view in a UNION ALL';
SELECT name FROM inline_union_v WHERE name = 'match';
SELECT name FROM inline_union_v WHERE name = 'match';
SELECT name FROM inline_union_v WHERE name = 'match';
SELECT name FROM inline_union_v WHERE name = 'match';

DROP VIEW inline_subquery_v;
DROP VIEW inline_union_v;
DROP VIEW inline_plain_v;
DROP VIEW inline_nondeterministic_v;
DROP DICTIONARY inline_dict;
DROP TABLE inline_ref;
DROP TABLE inline_data;
