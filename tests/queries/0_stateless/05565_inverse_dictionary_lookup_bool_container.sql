-- Tags: no-parallel-replicas
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.
-- The inverse dictionary lookup keeps the result of `=` on `Bool` attributes.

DROP DICTIONARY IF EXISTS d_bool;
DROP TABLE IF EXISTS src_bool;
DROP TABLE IF EXISTS probe;
CREATE TABLE src_bool (id UInt64, b Bool, ab Array(Nullable(Bool))) ENGINE = Memory;
INSERT INTO src_bool VALUES (1, true, [true]), (2, false, [false]);
CREATE DICTIONARY d_bool (id UInt64, b Bool, ab Array(Nullable(Bool))) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src_bool')) LAYOUT(HASHED()) LIFETIME(0);
CREATE TABLE probe (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO probe VALUES (1), (2);

SELECT 'ab = [2] on', count() FROM probe WHERE dictGet('d_bool', 'ab', id) = [toUInt8(2)] SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT 'ab = [2] off', count() FROM probe WHERE dictGet('d_bool', 'ab', id) = [toUInt8(2)] SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT 'b = Nullable 2 on', count() FROM probe WHERE dictGet('d_bool', 'b', id) = CAST(2, 'Nullable(UInt8)') SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT 'b = Nullable 2 off', count() FROM probe WHERE dictGet('d_bool', 'b', id) = CAST(2, 'Nullable(UInt8)') SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT 'b = 1 on', count() FROM probe WHERE dictGet('d_bool', 'b', id) = 1 SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT 'b = 1 off', count() FROM probe WHERE dictGet('d_bool', 'b', id) = 1 SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT 'ab = [true] on', count() FROM probe WHERE dictGet('d_bool', 'ab', id) = [true] SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT 'ab = [true] off', count() FROM probe WHERE dictGet('d_bool', 'ab', id) = [true] SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT 'b = 2 on', count() FROM probe WHERE dictGet('d_bool', 'b', id) = 2 SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT 'b = 2 off', count() FROM probe WHERE dictGet('d_bool', 'b', id) = 2 SETTINGS optimize_inverse_dictionary_lookup = 0;
SET optimize_inverse_dictionary_lookup = 1;
SELECT 'b = 1 plan', countIf(explain ILIKE '%dictGet%' OR explain ILIKE '%dictionary(%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM probe WHERE dictGet('d_bool', 'b', id) = 1);
SELECT 'ab = [2] plan', countIf(explain ILIKE '%dictGet%' OR explain ILIKE '%dictionary(%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM probe WHERE dictGet('d_bool', 'ab', id) = [toUInt8(2)]);

DROP DICTIONARY d_bool;
DROP TABLE src_bool;
DROP TABLE probe;
