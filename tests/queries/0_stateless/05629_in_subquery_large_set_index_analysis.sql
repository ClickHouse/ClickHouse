-- Index analysis sorts a set of `IN` with more than 131072 elements on several threads. Each key type below gets
-- a set of 300000 elements that matches three narrow ranges of the key, so the primary key must prune, and the
-- pruning must neither drop matching rows nor differ from the single-threaded sort.

DROP TABLE IF EXISTS t_u64;
DROP TABLE IF EXISTS t_str;
DROP TABLE IF EXISTS t_nullable;
DROP TABLE IF EXISTS t_lc;
DROP TABLE IF EXISTS t_tuple;

CREATE TABLE t_u64 (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024;
CREATE TABLE t_str (k String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024;
CREATE TABLE t_nullable (k Nullable(String)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024, allow_nullable_key = 1;
CREATE TABLE t_lc (k LowCardinality(String)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024;
CREATE TABLE t_tuple (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1024;

INSERT INTO t_u64 SELECT number FROM numbers(1000000);
INSERT INTO t_str SELECT leftPad(toString(number), 7, '0') FROM numbers(1000000);
INSERT INTO t_nullable SELECT if(number >= 990000, NULL, leftPad(toString(number), 7, '0')) FROM numbers(1000000);
INSERT INTO t_lc SELECT leftPad(toString(number), 7, '0') FROM numbers(1000000);
INSERT INTO t_tuple SELECT intDiv(number, 1000), number % 1000 FROM numbers(1000000);

-- The set: [100000, 200000), [450000, 550000) and [800000, 900000), in a scrambled order.
DROP VIEW IF EXISTS set_numbers;
CREATE VIEW set_numbers AS SELECT n + 100000 + intDiv(n, 100000) * 250000 AS x FROM (SELECT number * 7919 % 300000 AS n FROM numbers(300000));

SELECT 'UInt64';
SELECT count() FROM t_u64 WHERE k IN (SELECT x FROM set_numbers) SETTINGS max_threads = 8;
SELECT count() FROM t_u64 WHERE k IN (SELECT x FROM set_numbers) SETTINGS use_index_for_in_with_subqueries = 0;
SELECT g8.granules = g1.granules, g8.selected < g8.total
FROM
    (SELECT trimBoth(splitByString('Granules:', explain)[2]) AS granules, toUInt64(splitByChar('/', granules)[1]) AS selected, toUInt64(splitByChar('/', granules)[2]) AS total
     FROM (EXPLAIN indexes = 1 SELECT count() FROM t_u64 WHERE k IN (SELECT x FROM set_numbers) SETTINGS max_threads = 8, use_query_condition_cache = 0)
     WHERE explain ILIKE '%Granules: %/%') AS g8,
    (SELECT trimBoth(splitByString('Granules:', explain)[2]) AS granules
     FROM (EXPLAIN indexes = 1 SELECT count() FROM t_u64 WHERE k IN (SELECT x FROM set_numbers) SETTINGS max_threads = 1, use_query_condition_cache = 0)
     WHERE explain ILIKE '%Granules: %/%') AS g1;

SELECT 'String';
SELECT count() FROM t_str WHERE k IN (SELECT leftPad(toString(x), 7, '0') FROM set_numbers) SETTINGS max_threads = 8;
SELECT count() FROM t_str WHERE k IN (SELECT leftPad(toString(x), 7, '0') FROM set_numbers) SETTINGS use_index_for_in_with_subqueries = 0;
SELECT g8.granules = g1.granules, g8.selected < g8.total
FROM
    (SELECT trimBoth(splitByString('Granules:', explain)[2]) AS granules, toUInt64(splitByChar('/', granules)[1]) AS selected, toUInt64(splitByChar('/', granules)[2]) AS total
     FROM (EXPLAIN indexes = 1 SELECT count() FROM t_str WHERE k IN (SELECT leftPad(toString(x), 7, '0') FROM set_numbers) SETTINGS max_threads = 8, use_query_condition_cache = 0)
     WHERE explain ILIKE '%Granules: %/%') AS g8,
    (SELECT trimBoth(splitByString('Granules:', explain)[2]) AS granules
     FROM (EXPLAIN indexes = 1 SELECT count() FROM t_str WHERE k IN (SELECT leftPad(toString(x), 7, '0') FROM set_numbers) SETTINGS max_threads = 1, use_query_condition_cache = 0)
     WHERE explain ILIKE '%Granules: %/%') AS g1;

SELECT 'Nullable(String)';
SELECT count() FROM t_nullable WHERE k IN (SELECT if(x = 100000, NULL, leftPad(toString(x), 7, '0')) FROM set_numbers) SETTINGS max_threads = 8, transform_null_in = 1;
SELECT count() FROM t_nullable WHERE k IN (SELECT if(x = 100000, NULL, leftPad(toString(x), 7, '0')) FROM set_numbers) SETTINGS use_index_for_in_with_subqueries = 0, transform_null_in = 1;
SELECT count() FROM t_nullable WHERE k IN (SELECT leftPad(toString(x), 7, '0') FROM set_numbers) SETTINGS max_threads = 8;
SELECT count() FROM t_nullable WHERE k IN (SELECT leftPad(toString(x), 7, '0') FROM set_numbers) SETTINGS use_index_for_in_with_subqueries = 0;
SELECT g8.granules = g1.granules, g8.selected < g8.total
FROM
    (SELECT trimBoth(splitByString('Granules:', explain)[2]) AS granules, toUInt64(splitByChar('/', granules)[1]) AS selected, toUInt64(splitByChar('/', granules)[2]) AS total
     FROM (EXPLAIN indexes = 1 SELECT count() FROM t_nullable WHERE k IN (SELECT leftPad(toString(x), 7, '0') FROM set_numbers) SETTINGS max_threads = 8, use_query_condition_cache = 0)
     WHERE explain ILIKE '%Granules: %/%') AS g8,
    (SELECT trimBoth(splitByString('Granules:', explain)[2]) AS granules
     FROM (EXPLAIN indexes = 1 SELECT count() FROM t_nullable WHERE k IN (SELECT leftPad(toString(x), 7, '0') FROM set_numbers) SETTINGS max_threads = 1, use_query_condition_cache = 0)
     WHERE explain ILIKE '%Granules: %/%') AS g1;

SELECT 'LowCardinality(String)';
SELECT count() FROM t_lc WHERE k IN (SELECT leftPad(toString(x), 7, '0') FROM set_numbers) SETTINGS max_threads = 8;
SELECT count() FROM t_lc WHERE k IN (SELECT leftPad(toString(x), 7, '0') FROM set_numbers) SETTINGS use_index_for_in_with_subqueries = 0;
SELECT g8.granules = g1.granules, g8.selected < g8.total
FROM
    (SELECT trimBoth(splitByString('Granules:', explain)[2]) AS granules, toUInt64(splitByChar('/', granules)[1]) AS selected, toUInt64(splitByChar('/', granules)[2]) AS total
     FROM (EXPLAIN indexes = 1 SELECT count() FROM t_lc WHERE k IN (SELECT leftPad(toString(x), 7, '0') FROM set_numbers) SETTINGS max_threads = 8, use_query_condition_cache = 0)
     WHERE explain ILIKE '%Granules: %/%') AS g8,
    (SELECT trimBoth(splitByString('Granules:', explain)[2]) AS granules
     FROM (EXPLAIN indexes = 1 SELECT count() FROM t_lc WHERE k IN (SELECT leftPad(toString(x), 7, '0') FROM set_numbers) SETTINGS max_threads = 1, use_query_condition_cache = 0)
     WHERE explain ILIKE '%Granules: %/%') AS g1;

SELECT 'Tuple';
SELECT count() FROM t_tuple WHERE (a, b) IN (SELECT intDiv(x, 1000), x % 1000 FROM set_numbers) SETTINGS max_threads = 8;
SELECT count() FROM t_tuple WHERE (a, b) IN (SELECT intDiv(x, 1000), x % 1000 FROM set_numbers) SETTINGS use_index_for_in_with_subqueries = 0;
SELECT g8.granules = g1.granules, g8.selected < g8.total
FROM
    (SELECT trimBoth(splitByString('Granules:', explain)[2]) AS granules, toUInt64(splitByChar('/', granules)[1]) AS selected, toUInt64(splitByChar('/', granules)[2]) AS total
     FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tuple WHERE (a, b) IN (SELECT intDiv(x, 1000), x % 1000 FROM set_numbers) SETTINGS max_threads = 8, use_query_condition_cache = 0)
     WHERE explain ILIKE '%Granules: %/%') AS g8,
    (SELECT trimBoth(splitByString('Granules:', explain)[2]) AS granules
     FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tuple WHERE (a, b) IN (SELECT intDiv(x, 1000), x % 1000 FROM set_numbers) SETTINGS max_threads = 1, use_query_condition_cache = 0)
     WHERE explain ILIKE '%Granules: %/%') AS g1;

DROP VIEW set_numbers;
DROP TABLE t_u64;
DROP TABLE t_str;
DROP TABLE t_nullable;
DROP TABLE t_lc;
DROP TABLE t_tuple;
