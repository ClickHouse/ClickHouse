-- Tags: no-parallel-replicas
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

-- `optimize_inverse_dictionary_lookup` looks through a view column only in `WHERE` and `PREWHERE`. An expression with
-- an alias is one node shared by every section that uses the alias, so a rewrite of it in `WHERE` must not reach
-- `GROUP BY` or `HAVING`, where the key column of the view is not available after aggregation. Every query must give
-- the same result with the setting off and on.

SET enable_analyzer = 1;

DROP VIEW IF EXISTS shared_alias_v;
DROP DICTIONARY IF EXISTS shared_alias_dict;
DROP TABLE IF EXISTS shared_alias_ref;
DROP TABLE IF EXISTS shared_alias_data;

CREATE TABLE shared_alias_ref (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO shared_alias_ref VALUES (1, 'match'), (2, 'other');

CREATE DICTIONARY shared_alias_dict (id UInt64, name String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'shared_alias_ref')) LAYOUT(HASHED()) LIFETIME(0);

CREATE TABLE shared_alias_data (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO shared_alias_data VALUES (1), (1), (2), (3);

CREATE VIEW shared_alias_v SQL SECURITY INVOKER AS
SELECT id, dictGetString('shared_alias_dict', 'name', id) AS name FROM shared_alias_data;

WITH name = 'other' AS pred SELECT count() FROM shared_alias_v WHERE pred GROUP BY name HAVING pred
SETTINGS analyzer_inline_views = 0, optimize_inverse_dictionary_lookup = 0;
WITH name = 'other' AS pred SELECT count() FROM shared_alias_v WHERE pred GROUP BY name HAVING pred
SETTINGS analyzer_inline_views = 0, optimize_inverse_dictionary_lookup = 1;
-- The shared comparison is not rewritten in any section.
SELECT count() FROM (EXPLAIN QUERY TREE WITH name = 'other' AS pred SELECT count() FROM shared_alias_v WHERE pred GROUP BY name HAVING pred
SETTINGS analyzer_inline_views = 0, optimize_inverse_dictionary_lookup = 1) WHERE explain LIKE '%column_name: id%';

SELECT (name = 'match') AS c, count() FROM shared_alias_v WHERE c GROUP BY c ORDER BY c SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT (name = 'match') AS c, count() FROM shared_alias_v WHERE c GROUP BY c ORDER BY c SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT (name = 'match' AND id > 0) AS c, count() FROM shared_alias_v WHERE c GROUP BY c ORDER BY c SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT (name = 'match' AND id > 0) AS c, count() FROM shared_alias_v WHERE c GROUP BY c ORDER BY c SETTINGS optimize_inverse_dictionary_lookup = 1;

SELECT (name = 'match' OR id = 3) AS c, count() FROM shared_alias_v WHERE c GROUP BY c WITH ROLLUP ORDER BY c, count() SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT (name = 'match' OR id = 3) AS c, count() FROM shared_alias_v WHERE c GROUP BY c WITH ROLLUP ORDER BY c, count() SETTINGS optimize_inverse_dictionary_lookup = 1;

-- Without a shared alias the rewrite still applies: the filter is on the key column of the view.
SELECT name, count() FROM shared_alias_v WHERE name = 'match' GROUP BY name ORDER BY name SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT name, count() FROM shared_alias_v WHERE name = 'match' GROUP BY name SETTINGS optimize_inverse_dictionary_lookup = 1) WHERE explain LIKE '%column_name: id%';

DROP VIEW shared_alias_v;
DROP DICTIONARY shared_alias_dict;
DROP TABLE shared_alias_ref;
DROP TABLE shared_alias_data;
