-- The indexes over all columns of an `IN` set, in their order and unconverted, share one sorted copy of the set
-- (primary key, part pruning by statistics, projections). The others sort their own copy.

DROP TABLE IF EXISTS t_in_shared_sort;
DROP TABLE IF EXISTS t_in_shared_sort_tuple;
DROP TABLE IF EXISTS t_in_shared_sort_part;

CREATE TABLE t_in_shared_sort (k UInt64, v UInt64, PROJECTION p (SELECT * ORDER BY v, k))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024, auto_statistics_types = 'basic';
SYSTEM STOP MERGES t_in_shared_sort;
INSERT INTO t_in_shared_sort SELECT number, number % 100 FROM numbers(0, 50000);
INSERT INTO t_in_shared_sort SELECT number, number % 100 FROM numbers(50000, 50000);
INSERT INTO t_in_shared_sort SELECT number, number % 100 FROM numbers(100000, 50000);
INSERT INTO t_in_shared_sort SELECT number, number % 100 FROM numbers(150000, 50000);

-- The keys 80000 .. 109999 in a scrambled order (7919 is prime). With parallel replicas, only the local plan shows
-- the index analysis in `EXPLAIN`.
SELECT 'shared', count() FROM t_in_shared_sort
WHERE k IN (SELECT toUInt64(80000 + number * 7919 % 30000) FROM numbers(30000))
SETTINGS use_query_condition_cache = 0, max_threads = 4;

WITH (
    SELECT groupArray(explain) FROM (
        EXPLAIN indexes = 1 SELECT count() FROM t_in_shared_sort
        WHERE k IN (SELECT toUInt64(80000 + number * 7919 % 30000) FROM numbers(30000))
        SETTINGS use_query_condition_cache = 0, max_threads = 4, parallel_replicas_local_plan = 1)
) AS lines,
arrayFirst(x -> x ILIKE '%Granules: %/%', arraySlice(lines, arrayFirstIndex(x -> x ILIKE '%PrimaryKey%', lines))) AS granules
SELECT 'shared primary key prunes', toUInt64(extract(granules, 'Granules: (\\d+)/')) < toUInt64(extract(granules, 'Granules: \\d+/(\\d+)'));

SELECT 'cast', count() FROM t_in_shared_sort
WHERE k IN (SELECT toUInt32(80000 + number * 7919 % 30000) FROM numbers(30000))
SETTINGS use_query_condition_cache = 0, max_threads = 4;

CREATE TABLE t_in_shared_sort_tuple (a UInt64, b UInt64, c UInt64)
ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1024;
INSERT INTO t_in_shared_sort_tuple SELECT intDiv(number, 100), number % 100, number FROM numbers(200000);

SELECT 'tuple', count() FROM t_in_shared_sort_tuple
WHERE (a, b) IN (SELECT intDiv(x, 100), toUInt64(x % 100) FROM (SELECT toUInt64(80000 + number * 7919 % 30000) AS x FROM numbers(30000)))
SETTINGS use_query_condition_cache = 0, max_threads = 4;

SELECT 'reordered tuple', count() FROM t_in_shared_sort_tuple
WHERE (b, a) IN (SELECT toUInt64(x % 100), intDiv(x, 100) FROM (SELECT toUInt64(80000 + number * 7919 % 30000) AS x FROM numbers(30000)))
SETTINGS use_query_condition_cache = 0, max_threads = 4;

WITH (
    SELECT groupArray(explain) FROM (
        EXPLAIN indexes = 1 SELECT count() FROM t_in_shared_sort_tuple
        WHERE (b, a) IN (SELECT toUInt64(x % 100), intDiv(x, 100) FROM (SELECT toUInt64(80000 + number * 7919 % 30000) AS x FROM numbers(30000)))
        SETTINGS use_query_condition_cache = 0, max_threads = 4, parallel_replicas_local_plan = 1)
) AS lines,
arrayFirst(x -> x ILIKE '%Granules: %/%', arraySlice(lines, arrayFirstIndex(x -> x ILIKE '%PrimaryKey%', lines))) AS granules
SELECT 'reordered tuple primary key prunes', toUInt64(extract(granules, 'Granules: (\\d+)/')) < toUInt64(extract(granules, 'Granules: \\d+/(\\d+)'));

SELECT 'subset of the key', count() FROM t_in_shared_sort_tuple
WHERE (a, c) IN (SELECT intDiv(x, 100), x FROM (SELECT toUInt64(80000 + number * 7919 % 30000) AS x FROM numbers(30000)))
SETTINGS use_query_condition_cache = 0, max_threads = 4;

CREATE TABLE t_in_shared_sort_part (p UInt8, k UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY k SETTINGS index_granularity = 1024;
INSERT INTO t_in_shared_sort_part SELECT number % 4, number FROM numbers(200000);

-- Index analysis of the parts runs on several threads, which take the shared copy concurrently.
SELECT 'concurrent', count() FROM t_in_shared_sort_part
WHERE k IN (SELECT toUInt64(80000 + number * 7919 % 30000) FROM numbers(30000))
SETTINGS use_constant_folding_in_index_analysis = 1, max_threads_for_indexes = 4, use_query_condition_cache = 0, max_threads = 4;

DROP TABLE t_in_shared_sort;
DROP TABLE t_in_shared_sort_tuple;
DROP TABLE t_in_shared_sort_part;
