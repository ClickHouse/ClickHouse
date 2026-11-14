-- Index analysis sorts a set of `IN` with more than 131072 elements on several threads. Each key type below gets
-- a set of 200000 elements that matches two narrow ranges of the key, so the primary key must prune, and the
-- pruning must neither drop matching rows nor differ from the single-threaded sort.

DROP TABLE IF EXISTS t_u64, t_str, t_nullable, t_tuple;

CREATE TABLE t_u64 (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024;
CREATE TABLE t_str (k String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024;
CREATE TABLE t_nullable (k Nullable(String)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024, allow_nullable_key = 1;
CREATE TABLE t_tuple (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1024;

INSERT INTO t_u64 SELECT number FROM numbers(400000);
INSERT INTO t_str SELECT leftPad(toString(number), 7, '0') FROM numbers(400000);
INSERT INTO t_nullable SELECT if(number >= 390000, NULL, leftPad(toString(number), 7, '0')) FROM numbers(400000);
INSERT INTO t_tuple SELECT intDiv(number, 1000), number % 1000 FROM numbers(400000);

-- The set: [50000, 150000) and [250000, 350000), in a scrambled order.
DROP VIEW IF EXISTS set_numbers;
CREATE VIEW set_numbers AS SELECT n + 50000 + intDiv(n, 100000) * 100000 AS x FROM (SELECT number * 7919 % 200000 AS n FROM numbers(200000));

SELECT 'UInt64';
SELECT count() FROM t_u64 WHERE k IN (SELECT x FROM set_numbers) SETTINGS max_threads = 8;
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
SELECT g8.granules = g1.granules, g8.selected < g8.total
FROM
    (SELECT trimBoth(splitByString('Granules:', explain)[2]) AS granules, toUInt64(splitByChar('/', granules)[1]) AS selected, toUInt64(splitByChar('/', granules)[2]) AS total
     FROM (EXPLAIN indexes = 1 SELECT count() FROM t_str WHERE k IN (SELECT leftPad(toString(x), 7, '0') FROM set_numbers) SETTINGS max_threads = 8, use_query_condition_cache = 0)
     WHERE explain ILIKE '%Granules: %/%') AS g8,
    (SELECT trimBoth(splitByString('Granules:', explain)[2]) AS granules
     FROM (EXPLAIN indexes = 1 SELECT count() FROM t_str WHERE k IN (SELECT leftPad(toString(x), 7, '0') FROM set_numbers) SETTINGS max_threads = 1, use_query_condition_cache = 0)
     WHERE explain ILIKE '%Granules: %/%') AS g1;

SELECT 'Nullable(String)';
SELECT count() FROM t_nullable WHERE k IN (SELECT if(x = 50000, NULL, leftPad(toString(x), 7, '0')) FROM set_numbers) SETTINGS max_threads = 8, transform_null_in = 1;
SELECT g8.granules = g1.granules, g8.selected < g8.total
FROM
    (SELECT trimBoth(splitByString('Granules:', explain)[2]) AS granules, toUInt64(splitByChar('/', granules)[1]) AS selected, toUInt64(splitByChar('/', granules)[2]) AS total
     FROM (EXPLAIN indexes = 1 SELECT count() FROM t_nullable WHERE k IN (SELECT if(x = 50000, NULL, leftPad(toString(x), 7, '0')) FROM set_numbers) SETTINGS max_threads = 8, transform_null_in = 1, use_query_condition_cache = 0)
     WHERE explain ILIKE '%Granules: %/%') AS g8,
    (SELECT trimBoth(splitByString('Granules:', explain)[2]) AS granules
     FROM (EXPLAIN indexes = 1 SELECT count() FROM t_nullable WHERE k IN (SELECT if(x = 50000, NULL, leftPad(toString(x), 7, '0')) FROM set_numbers) SETTINGS max_threads = 1, transform_null_in = 1, use_query_condition_cache = 0)
     WHERE explain ILIKE '%Granules: %/%') AS g1;

SELECT 'Tuple';
SELECT count() FROM t_tuple WHERE (a, b) IN (SELECT intDiv(x, 1000), x % 1000 FROM set_numbers) SETTINGS max_threads = 8;
SELECT g8.granules = g1.granules, g8.selected < g8.total
FROM
    (SELECT trimBoth(splitByString('Granules:', explain)[2]) AS granules, toUInt64(splitByChar('/', granules)[1]) AS selected, toUInt64(splitByChar('/', granules)[2]) AS total
     FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tuple WHERE (a, b) IN (SELECT intDiv(x, 1000), x % 1000 FROM set_numbers) SETTINGS max_threads = 8, use_query_condition_cache = 0)
     WHERE explain ILIKE '%Granules: %/%') AS g8,
    (SELECT trimBoth(splitByString('Granules:', explain)[2]) AS granules
     FROM (EXPLAIN indexes = 1 SELECT count() FROM t_tuple WHERE (a, b) IN (SELECT intDiv(x, 1000), x % 1000 FROM set_numbers) SETTINGS max_threads = 1, use_query_condition_cache = 0)
     WHERE explain ILIKE '%Granules: %/%') AS g1;

DROP VIEW set_numbers;
DROP TABLE t_u64, t_str, t_nullable, t_tuple;
