-- Tags: no-parallel-replicas
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

-- `optimize_inverse_dictionary_lookup` matches the key of `dictGet` with a column of the view in a tree resolved
-- for inspection only. A constant folded from a non-deterministic function, such as `randConstant`, can get another
-- value when the view is read, so the view is not looked through if the key or the column has one.

SET enable_analyzer = 1;
SET analyzer_inline_views = 0;

DROP VIEW IF EXISTS nondet_key_v;
DROP VIEW IF EXISTS nondet_key_projected_v;
DROP VIEW IF EXISTS nondet_key_lookup_v;
DROP VIEW IF EXISTS nondet_key_deterministic_v;
DROP DICTIONARY IF EXISTS nondet_key_dict;
DROP TABLE IF EXISTS nondet_key_ref;
DROP TABLE IF EXISTS nondet_key_data;

CREATE TABLE nondet_key_ref (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO nondet_key_ref VALUES (1, 'match'), (2, 'other');

CREATE DICTIONARY nondet_key_dict (id UInt64, name String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'nondet_key_ref')) LAYOUT(HASHED()) LIFETIME(0);

CREATE TABLE nondet_key_data (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO nondet_key_data VALUES (1), (2);

-- The offset of `k` is 0 or 1. If it is 0 for inspection, `k` is equal to the key of `dictGet`, but not when the view is read.
CREATE VIEW nondet_key_v SQL SECURITY INVOKER AS
SELECT id + toUInt64(randConstant() % 2) AS k, dictGetString('nondet_key_dict', 'name', id + toUInt64(0)) AS name FROM nondet_key_data;

SELECT count() FROM nondet_key_v WHERE name = 'match' SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM nondet_key_v WHERE name = 'match' SETTINGS optimize_inverse_dictionary_lookup = 1)
WHERE explain LIKE '%column_name: k,%';

-- The same with an offset that is always 0, but is still folded from `randConstant`: in the projected column.
CREATE VIEW nondet_key_projected_v SQL SECURITY INVOKER AS
SELECT id + toUInt64(randConstant() * 0) AS k, dictGetString('nondet_key_dict', 'name', id + toUInt64(0)) AS name FROM nondet_key_data;

SELECT count() FROM nondet_key_projected_v WHERE name = 'match' SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM nondet_key_projected_v WHERE name = 'match' SETTINGS optimize_inverse_dictionary_lookup = 1)
WHERE explain LIKE '%column_name: k,%';

-- In the key of `dictGet`.
CREATE VIEW nondet_key_lookup_v SQL SECURITY INVOKER AS
SELECT id + toUInt64(0) AS k, dictGetString('nondet_key_dict', 'name', id + toUInt64(randConstant() * 0)) AS name FROM nondet_key_data;

SELECT count() FROM nondet_key_lookup_v WHERE name = 'match' SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM nondet_key_lookup_v WHERE name = 'match' SETTINGS optimize_inverse_dictionary_lookup = 1)
WHERE explain LIKE '%column_name: k,%';

-- A constant folded from deterministic functions is the same in every analysis: the view is looked through.
CREATE VIEW nondet_key_deterministic_v SQL SECURITY INVOKER AS
SELECT id + toUInt64(0) AS k, dictGetString('nondet_key_dict', 'name', id + toUInt64(0)) AS name FROM nondet_key_data;

SELECT count() FROM nondet_key_deterministic_v WHERE name = 'match' SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT count() > 0 FROM (EXPLAIN QUERY TREE SELECT count() FROM nondet_key_deterministic_v WHERE name = 'match' SETTINGS optimize_inverse_dictionary_lookup = 1)
WHERE explain LIKE '%column_name: k,%';

DROP VIEW nondet_key_v;
DROP VIEW nondet_key_projected_v;
DROP VIEW nondet_key_lookup_v;
DROP VIEW nondet_key_deterministic_v;
DROP DICTIONARY nondet_key_dict;
DROP TABLE nondet_key_ref;
DROP TABLE nondet_key_data;
