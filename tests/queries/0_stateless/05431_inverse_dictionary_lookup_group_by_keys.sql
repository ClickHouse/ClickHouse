-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: Dictionary is not available on parallel-replica workers.

-- `optimize_inverse_dictionary_lookup` must not change a GROUP BY key that is read from the aggregation result.
-- Every query runs with the optimization on and off, and both must return the same rows.

SET rewrite_in_to_join = 0;
SET optimize_inverse_dictionary_lookup = 1;

DROP DICTIONARY IF EXISTS d;
DROP TABLE IF EXISTS src;
CREATE TABLE src (k UInt64, a String) ENGINE = Memory;
INSERT INTO src VALUES (1, 'p'), (2, 'q'), (5, 'p');
CREATE DICTIONARY d (k UInt64, a String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE 'src')) LAYOUT(HASHED()) LIFETIME(0);

SELECT '-- comparison key matching no dictionary key, CUBE, group_by_use_nulls';
SELECT dictGet('d', 'a', number) = 'zzz' AS k, 0 AS z, arrayMap(x -> x + z, [1]) AS m, count() FROM numbers(3)
GROUP BY k WITH CUBE ORDER BY ALL SETTINGS group_by_use_nulls = 1, optimize_inverse_dictionary_lookup = 1;
SELECT dictGet('d', 'a', number) = 'zzz' AS k, 0 AS z, arrayMap(x -> x + z, [1]) AS m, count() FROM numbers(3)
GROUP BY k WITH CUBE ORDER BY ALL SETTINGS group_by_use_nulls = 1, optimize_inverse_dictionary_lookup = 0;

SELECT '-- LIKE key, GROUPING SETS, group_by_use_nulls';
SELECT dictGet('d', 'a', number) LIKE 'p%' AS k, count() FROM numbers(6)
GROUP BY GROUPING SETS ((k), ()) ORDER BY ALL SETTINGS group_by_use_nulls = 1, optimize_inverse_dictionary_lookup = 1;
SELECT dictGet('d', 'a', number) LIKE 'p%' AS k, count() FROM numbers(6)
GROUP BY GROUPING SETS ((k), ()) ORDER BY ALL SETTINGS group_by_use_nulls = 1, optimize_inverse_dictionary_lookup = 0;

SELECT '-- comparisons of a dictGet key in SELECT and HAVING';
SELECT dictGet('d', 'a', number) AS v, v LIKE 'p%' AS l, count() FROM numbers(6)
GROUP BY v HAVING v = 'p' ORDER BY ALL SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT dictGet('d', 'a', number) AS v, v LIKE 'p%' AS l, count() FROM numbers(6)
GROUP BY v HAVING v = 'p' ORDER BY ALL SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT '-- comparison of a dictGet key in HAVING, ROLLUP';
SELECT dictGet('d', 'a', number) AS v, number, count() FROM numbers(6)
GROUP BY ROLLUP(v, number) HAVING v = 'p' ORDER BY ALL SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT dictGet('d', 'a', number) AS v, number, count() FROM numbers(6)
GROUP BY ROLLUP(v, number) HAVING v = 'p' ORDER BY ALL SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT '-- comparison of a dictGet key in LIMIT AFTER and LIMIT UNTIL';
SELECT dictGet('d', 'a', number) AS v, count() FROM numbers(6)
GROUP BY v ORDER BY v LIMIT 5 AFTER v = 'p' SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT dictGet('d', 'a', number) AS v, count() FROM numbers(6)
GROUP BY v ORDER BY v LIMIT 5 AFTER v = 'p' SETTINGS optimize_inverse_dictionary_lookup = 0;
SELECT dictGet('d', 'a', number) AS v, count() FROM numbers(6)
GROUP BY v ORDER BY v LIMIT 5 UNTIL v = 'p' SETTINGS optimize_inverse_dictionary_lookup = 1;
SELECT dictGet('d', 'a', number) AS v, count() FROM numbers(6)
GROUP BY v ORDER BY v LIMIT 5 UNTIL v = 'p' SETTINGS optimize_inverse_dictionary_lookup = 0;

SELECT '-- comparison key, ROLLUP, group_by_use_nulls, remote';
SELECT dictGet('d', 'a', dummy) = 'zzz' AS k, count() FROM remote('localhost,localhost', system.one)
GROUP BY k WITH ROLLUP ORDER BY ALL SETTINGS group_by_use_nulls = 1, optimize_inverse_dictionary_lookup = 1;
SELECT dictGet('d', 'a', dummy) = 'zzz' AS k, count() FROM remote('localhost,localhost', system.one)
GROUP BY k WITH ROLLUP ORDER BY ALL SETTINGS group_by_use_nulls = 1, optimize_inverse_dictionary_lookup = 0;

SELECT '-- WHERE and aggregate arguments are still rewritten, HAVING is not';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT dictGet('d', 'a', number) AS v, countIf(v = 'p') FROM numbers(6)
WHERE v = 'p' GROUP BY v WITH ROLLUP HAVING v = 'p') WHERE explain ILIKE '%function_name: in,%';

SELECT '-- a comparison key without group_by_use_nulls is still rewritten';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT dictGet('d', 'a', number) = 'p' AS k, count() FROM numbers(6)
GROUP BY k WITH ROLLUP SETTINGS group_by_use_nulls = 0) WHERE explain ILIKE '%function_name: in,%';

DROP DICTIONARY d;
DROP TABLE src;
