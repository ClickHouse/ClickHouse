-- `query_cache_use_only_when_data_was_not_changed` must not cache a result that can change while every
-- referenced table stays the same. `groupArraySample` without a seed is such a function, but it is known
-- only after the aggregate function is resolved, also when it runs through `arrayReduce`. The seeded
-- forms are deterministic and are cached: each query runs twice, and only the second run of a seeded
-- form is a cache hit.

-- The cache key includes the current database, so this test (running in its own database) does not
-- need to clear the server-wide query cache.

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t SELECT number FROM numbers(1000);

SET use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;

SELECT 'unseeded', length(groupArraySample(3)(x)) FROM t;
SELECT 'unseeded', length(groupArraySample(3)(x)) FROM t;
SELECT 'unseeded arrayReduce', length(arrayReduce('groupArraySample(3)', groupArray(x))) FROM t;
SELECT 'unseeded arrayReduce', length(arrayReduce('groupArraySample(3)', groupArray(x))) FROM t;
SELECT 'unseeded combinator', length(groupArraySampleIf(3)(x, x % 2 = 0)) FROM t;
SELECT 'unseeded combinator', length(groupArraySampleIf(3)(x, x % 2 = 0)) FROM t;

SELECT 'seeded', groupArraySample(3, 42)(x) = groupArraySample(3, 42)(x) FROM t;
SELECT 'seeded', groupArraySample(3, 42)(x) = groupArraySample(3, 42)(x) FROM t;
SELECT 'seeded arrayReduce', length(arrayReduce('groupArraySample(3, 42)', groupArray(x))) FROM t;
SELECT 'seeded arrayReduce', length(arrayReduce('groupArraySample(3, 42)', groupArray(x))) FROM t;

SET use_query_cache = 0;

SYSTEM FLUSH LOGS query_log;

SELECT extract(query, '^SELECT \'([a-zA-Z ]+)\''), ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND query LIKE 'SELECT \'%seeded%'
ORDER BY event_time_microseconds;

DROP TABLE t;
