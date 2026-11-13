-- Tags: shard
-- A predicate on an aggregate function of a `GROUP BY` key can become a predicate on the key, but not on the columns of the key.

SET enable_positional_arguments = 1;
SET optimize_aggregators_of_group_by_keys = 1, optimize_group_by_function_keys = 1, optimize_truncate_order_by_after_group_by_keys = 1, optimize_time_filter_with_preimage = 1;
SET optimize_inverse_dictionary_lookup = 1, optimize_or_like_chain = 1, optimize_or_like_chain_min_substrings = 2, optimize_min_equality_disjunction_chain_length = 3;
SET session_timezone = 'UTC';

DROP DICTIONARY IF EXISTS dict;
DROP TABLE IF EXISTS source;
DROP TABLE IF EXISTS t;

CREATE TABLE t (a UInt64, s String, d DateTime) ENGINE = Memory;
INSERT INTO t SELECT number % 4, ['x', 'y', 'xy', 'z'][number % 4 + 1], toDateTime('2024-12-31 00:00:00') + number % 4 * 43200 FROM numbers(8);
CREATE TABLE source (id UInt64, name String) ENGINE = Memory;
INSERT INTO source VALUES (0, 'p'), (1, 'q'), (2, 'p'), (3, 'r');
CREATE DICTIONARY dict (id UInt64, name String) PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'source' DATABASE currentDatabase())) LAYOUT(FLAT()) LIFETIME(0);

SELECT 'preimage';
SELECT toYear(d) AS k, max(k) = 2024 AS m, count() FROM t GROUP BY 1 ORDER BY 1;
SELECT toYear(d) AS k, max(k) = 2024 AS m, count() FROM t GROUP BY k ORDER BY k;
SELECT toDate(d) AS k, any(if(k = '2024-12-31', 'a', 'b')) AS m, count() FROM t GROUP BY 1 ORDER BY 1;
SELECT toDate(d) AS k, any(if(k = '2024-12-31', 'a', 'b')) AS m, count() FROM t GROUP BY k ORDER BY k;
SELECT toYear(d) = 2024 AS k, count() FROM t GROUP BY 1 HAVING max(k) = 1;
SELECT toYear(d) = 2024 AS k, count() FROM t GROUP BY k HAVING max(k) = 1;
SELECT toYYYYMM(d) AS k, min(k) < 202501 AS m, count() FROM remote('127.0.0.{2,3}', currentDatabase(), t) GROUP BY 1 ORDER BY 1;
SELECT toYYYYMM(d) AS k, min(k) < 202501 AS m, count() FROM remote('127.0.0.2', currentDatabase(), t) GROUP BY 1 ORDER BY min(k) < 202501, k;
SELECT toYear(d) AS k, d AS x, max(k) = 2024 AS m FROM t WHERE a = 1 GROUP BY GROUPING SETS ((1), (1, 2)) ORDER BY ALL;
SELECT toYear(d) AS k, d AS x, max(k) = 2024 AS m FROM t WHERE a = 1 GROUP BY GROUPING SETS ((k), (k, x)) ORDER BY ALL;

SELECT 'dictGet';
SELECT dictGet('dict', 'name', a) AS k, max(k) = 'p' AS m, count() FROM t GROUP BY 1 ORDER BY 1;
SELECT dictGet('dict', 'name', a) AS k, max(k) = 'p' AS m, count() FROM t GROUP BY k ORDER BY k;
SELECT dictGet('dict', 'name', a) AS k, count() FROM t GROUP BY 1 HAVING any(k IN ('p', 'r')) ORDER BY 1;

SELECT 'OR and AND';
SELECT a = 0 AS x, a = 1 AS y, a = 2 AS z, any(x OR y OR z) AS m, any(not x AND not y AND not z) AS n, count() FROM t GROUP BY 1, 2, 3 ORDER BY ALL;
SELECT a = 0 AS x, a = 1 AS y, a = 2 AS z, any(x OR y OR z) AS m, any(not x AND not y AND not z) AS n, count() FROM t GROUP BY x, y, z ORDER BY ALL;
SELECT a != 0 AS x, a != 1 AS y, a != 2 AS z, (x AND y AND z) AS m, count() FROM t GROUP BY 1, 2, 3, 4 ORDER BY ALL;
SELECT a != 0 AS x, a != 1 AS y, a != 2 AS z, (x AND y AND z) AS m, count() FROM t GROUP BY x, y, z, m ORDER BY ALL;
SELECT (a = 0 OR a = 1 OR a = 2) AS k, max(k) = 1 AS m, if(k = 1, 'a', 'b') AS n, count() FROM t GROUP BY 1, 3 ORDER BY 1;
SELECT (a = 0 OR a = 1 OR a = 2) AS k, max(k) = 1 AS m, if(k = 1, 'a', 'b') AS n, count() FROM t GROUP BY k, n ORDER BY k;
SELECT (a = 3 AND a > 0) AS k, count() FROM t GROUP BY 1 HAVING max(k) = 1;
SELECT (a = 3 AND a > 0) AS k, count() FROM t GROUP BY k HAVING max(k) = 1;
SELECT a = 0 AS x, a = 1 AS y, a = 2 AS z, any(x OR y OR z) AS m, count() FROM remote('127.0.0.{2,3}', currentDatabase(), t) GROUP BY 1, 2, 3 ORDER BY ALL;
SELECT a = 0 AS x, a = 1 AS y, a = 2 AS z, any(x OR y OR z) AS m, count() FROM remote('127.0.0.2', currentDatabase(), t) GROUP BY 1, 2, 3 ORDER BY ALL;

SELECT 'LIKE';
SELECT s LIKE '%x%' AS x, s LIKE '%y%' AS y, s LIKE '%z%' AS z, any(x OR y OR z) AS m, count() FROM t GROUP BY 1, 2, 3 ORDER BY ALL;
SELECT s LIKE '%x%' AS x, s LIKE '%y%' AS y, s LIKE '%z%' AS z, any(x OR y OR z) AS m, count() FROM t GROUP BY x, y, z ORDER BY ALL;
SELECT s LIKE '%x%' AS x, s LIKE '%y%' AS y, s LIKE '%z%' AS z, (x OR y OR z) AS m, count() FROM t GROUP BY 1, 2, 3, 4 ORDER BY ALL;

SELECT 'in other functions';
SELECT if(toYear(d) = 2024, 'new', 'old') AS k, any(k) = 'new' AS m, count() FROM t GROUP BY 1 ORDER BY 1;
SELECT if(toYear(d) = 2024, 'new', 'old') AS k, any(k) = 'new' AS m, count() FROM t GROUP BY k ORDER BY 1;
SELECT concat(toString(toYear(d)), if(toYear(d) = 2024, '*', '')) AS k, max(k) >= '2025' AS m, count() FROM t GROUP BY 1 ORDER BY 1;
SELECT concat(toString(toYear(d)), if(toYear(d) = 2024, '*', '')) AS k, max(k) >= '2025' AS m, count() FROM t GROUP BY k ORDER BY 1;
SELECT if(1, a = 0, 0) AS x, if(1, a = 1, 0) AS y, if(1, a = 2, 0) AS z, max(x) OR max(y) OR max(z) AS m, count() FROM t GROUP BY 1, 2, 3 ORDER BY ALL;
SELECT if(1, a = 0, 0) AS x, if(1, a = 1, 0) AS y, if(1, a = 2, 0) AS z, max(x) OR max(y) OR max(z) AS m, count() FROM t GROUP BY x, y, z ORDER BY ALL;

SELECT 'ORDER BY';
SELECT toYear(d) AS k, a % 2 AS x, k + x AS z, count() FROM t GROUP BY 1, 2, 3 ORDER BY 1, 2, k = 2024 DESC;
SELECT toYear(d) AS k, a % 2 AS x, k + x AS z, count() FROM t GROUP BY k, x, z ORDER BY k, x, k = 2024 DESC;
SELECT a = 0 AS x, a = 1 AS y, a = 2 AS z, if(x OR y, 'p', 'q') AS w, count() FROM t GROUP BY 1, 2, 3, 4 ORDER BY 1, 2, 3, NOT (x OR y OR z);
SELECT a = 0 AS x, a = 1 AS y, a = 2 AS z, if(x OR y, 'p', 'q') AS w, count() FROM t GROUP BY x, y, z, w ORDER BY x, y, z, NOT (x OR y OR z);
SELECT s = 'x' AS k, NOT k AS x, count() FROM t GROUP BY 1, 2 ORDER BY 1, k OR k;
SELECT s = 'x' AS k, NOT k AS x, count() FROM t GROUP BY k, x ORDER BY k, k OR k;
SELECT toYear(d) AS k, toFloat64(a) AS f, count() FROM t GROUP BY 1, 2 ORDER BY 1, 2, k = 2024;
SELECT toYear(d) AS k, toFloat64(a) AS f, count() FROM t GROUP BY k, f ORDER BY k, f, k = 2024;
SELECT toYear(d) AS k, min(k) AS m, count() FROM t GROUP BY 1 ORDER BY 2, k = 2024;
SELECT toYear(d) AS k, min(k) AS m, count() FROM t GROUP BY k ORDER BY 2, k;
SELECT toYear(d) AS k, count() FROM t GROUP BY 1 ORDER BY 1, k = 2024 SETTINGS distributed_group_by_no_merge = 1;
SELECT toYear(d) AS k, count() FROM t GROUP BY k ORDER BY k, k = 2024 SETTINGS distributed_group_by_no_merge = 1;
SELECT toYear(d) AS k, count() FROM t GROUP BY 1 HAVING arrayJoin([4, 4]) = count() ORDER BY 1, k = 2024;
SELECT toYear(d) AS k, count() FROM t GROUP BY k HAVING arrayJoin([4, 4]) = count() ORDER BY k, k = 2024;

SELECT 'sort keys';
SELECT toYear(d) AS k, a, count() FROM t GROUP BY k, a ORDER BY k, k = 2024, a;
SELECT toYear(d) AS k, toFloat64(a) AS f, count() FROM t GROUP BY k, f ORDER BY f, k, if(k = 2024, count(), 0);
SELECT toYear(d) AS k, toFloat64(a) AS f, count() FROM t GROUP BY k, f ORDER BY f, k, (k = 2024) IN (SELECT 1), arrayMap(x -> k = 2024, [1]);
SELECT toYear(d) AS k, count() FROM t GROUP BY k ORDER BY k, rowNumberInAllBlocks(), if(k = 2024, count(), 0);
SELECT toYear(d) AS k, count() FROM remote('127.0.0.{2,3}', currentDatabase(), t) GROUP BY k ORDER BY k, if(k = 2024, count(), 0) SETTINGS distributed_group_by_no_merge = 2;
SELECT toYear(d) AS k, arrayJoin([a, 2]) AS x, arrayJoin([a, toUInt8(2)]) AS y, count() FROM t GROUP BY k, x, y ORDER BY k, x, y, k = 2024;
SELECT DISTINCT count() > 0 FROM t GROUP BY toYear(d) AS k, rand(a + 2) AS x, rand(a + toUInt8(2)) AS y ORDER BY k, x, y, k = 2024;

SELECT 'rewritten';
SELECT countIf(explain LIKE '%toYear(%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT toYear(d) = 2024 AS k, count() FROM t GROUP BY 1);
SELECT countIf(explain LIKE '%toYear(%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM t GROUP BY a HAVING max(toYear(d) = 2024));
SELECT countIf(explain LIKE '% OR %') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT (a = 0 OR a = 1 OR a = 2) AS k, count() FROM t GROUP BY 1);
SELECT countIf(explain LIKE '%max(%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT a + 1 AS k, count() FROM t GROUP BY k HAVING max(k) < 3);
SELECT countIf(explain LIKE '%max%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT if(a > 1, a, NULL) AS k, count() FROM t GROUP BY k HAVING max(k) < 3);
SELECT countIf(explain LIKE '%max(%') FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT toYear(d) AS k, count() FROM t GROUP BY k HAVING max(k) = 2024);

DROP DICTIONARY dict;
DROP TABLE source;
DROP TABLE t;
