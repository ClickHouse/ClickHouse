-- Index analysis sorts a set of `IN` with more than 131072 elements on several threads. Each key type below gets
-- a set of 200000 elements that matches two narrow ranges of the key, so the primary key must prune, and the
-- pruning must neither drop matching rows nor differ from the single-threaded sort.

DROP TABLE IF EXISTS t_u64, t_str, t_nullable, t_tuple, t_partitioned;

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

SELECT 'Constant folding';
-- With `use_constant_folding_in_index_analysis`, each partition gets its own condition, built on a thread of the parallel
-- per-part analysis, so the set is sorted on the `SortBlock` pool only without it.
CREATE TABLE t_partitioned (k UInt64) ENGINE = MergeTree PARTITION BY intDiv(k, 100000) ORDER BY k SETTINGS index_granularity = 1024;
INSERT INTO t_partitioned SELECT number FROM numbers(400000);
SET log_queries = 1, log_query_threads = 1, log_queries_min_query_duration_ms = 0;
SELECT count() FROM t_partitioned WHERE k IN (SELECT x FROM set_numbers)
SETTINGS max_threads = 4, max_threads_for_indexes = 4, use_query_condition_cache = 0, use_constant_folding_in_index_analysis = 0, log_comment = '05629_no_folding';
SELECT count() FROM t_partitioned WHERE k IN (SELECT x FROM set_numbers)
SETTINGS max_threads = 4, max_threads_for_indexes = 4, use_query_condition_cache = 0, use_constant_folding_in_index_analysis = 1, log_comment = '05629_folding';
SYSTEM FLUSH LOGS query_log, query_thread_log;
SELECT
    log_comment,
    query_id IN (
        SELECT query_id
        FROM system.query_thread_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase() AND thread_name = 'SortBlock'
    ) AS sorted_on_pool
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05629_no_folding', '05629_folding')
ORDER BY log_comment DESC;

DROP VIEW set_numbers;
DROP TABLE t_u64, t_str, t_nullable, t_tuple, t_partitioned;
