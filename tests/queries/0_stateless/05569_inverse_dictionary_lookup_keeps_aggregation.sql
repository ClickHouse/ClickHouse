-- Random settings limits: optimize_inverse_dictionary_lookup=(1, None)
-- The limit keeps the rewrite under test enabled.
-- A dictGet comparison that no dictionary key satisfies must keep the aggregate functions and arrayJoin of its key.

DROP DICTIONARY IF EXISTS d_simple_keep_agg;
DROP DICTIONARY IF EXISTS d_complex_keep_agg;
DROP TABLE IF EXISTS src_simple_keep_agg;
DROP TABLE IF EXISTS src_complex_keep_agg;

CREATE TABLE src_simple_keep_agg (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
INSERT INTO src_simple_keep_agg VALUES (1, 'a'), (2, 'b'), (3, 'c');
CREATE DICTIONARY d_simple_keep_agg (id UInt64, name String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src_simple_keep_agg')) LAYOUT(HASHED()) LIFETIME(0);

CREATE TABLE src_complex_keep_agg (k1 UInt64, k2 String, name String) ENGINE = MergeTree ORDER BY k1;
INSERT INTO src_complex_keep_agg VALUES (1, 'x', 'a'), (2, 'x', 'b');
CREATE DICTIONARY d_complex_keep_agg (k1 UInt64, k2 String, name String)
PRIMARY KEY k1, k2 SOURCE(CLICKHOUSE(TABLE 'src_complex_keep_agg')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);

SELECT dictGet('d_simple_keep_agg', 'name', count()) = 'zzz' FROM numbers(5);
SELECT count() FROM (SELECT 'zzz' = dictGetString('d_simple_keep_agg', 'name', sum(number)) FROM numbers(5));
SELECT count() FROM (SELECT dictGet('d_simple_keep_agg', 'name', count()) = 'zzz' FROM numbers(0));
SELECT (SELECT dictGet('d_simple_keep_agg', 'name', count()) = 'zzz' FROM numbers(5));
SELECT count() FROM (SELECT 1 FROM numbers(5) HAVING NOT (dictGet('d_simple_keep_agg', 'name', count()) = 'zzz'));
SELECT count() FROM (SELECT 1 FROM numbers(5) ORDER BY dictGet('d_simple_keep_agg', 'name', count()) = 'zzz');
SELECT count() FROM (SELECT dictGet('d_complex_keep_agg', 'name', (count(), 'x')) = 'zzz' FROM numbers(5));
SELECT count() FROM (SELECT dictGet('d_simple_keep_agg', 'name', count()) = 'zzz' FROM remote('127.0.0.{1,2}', system.one));
SELECT count() FROM (SELECT dictGet('d_simple_keep_agg', 'name', toUInt64(arrayJoin([1, 2, 3]))) = 'zzz');
SELECT count() FROM numbers(1) WHERE dictGet('d_simple_keep_agg', 'name', toUInt64(arrayJoin([1, 2, 3]))) = 'zzz' OR number = 0;

-- The comparison is still rewritten without a dictionary lookup.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT dictGet('d_simple_keep_agg', 'name', count()) = 'zzz' FROM numbers(5)) WHERE explain ILIKE '%function_name: dictGet%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT dictGet('d_simple_keep_agg', 'name', count()) = 'zzz' FROM numbers(5)) WHERE explain ILIKE '%function_name: in,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT dictGet('d_simple_keep_agg', 'name', toUInt64(arrayJoin([1, 2, 3]))) = 'zzz') WHERE explain ILIKE '%function_name: dictGet%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT dictGet('d_simple_keep_agg', 'name', toUInt64(arrayJoin([1, 2, 3]))) = 'zzz') WHERE explain ILIKE '%function_name: in,%';

DROP DICTIONARY d_simple_keep_agg;
DROP DICTIONARY d_complex_keep_agg;
DROP TABLE src_simple_keep_agg;
DROP TABLE src_complex_keep_agg;
