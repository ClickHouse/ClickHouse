-- `query_cache_use_only_when_data_was_not_changed` must not cache a result that can change while every
-- referenced table stays the same. The predicates of `additional_table_filters` and `additional_result_filter`
-- are resolved into their own trees, and `arrayReduce('groupArraySample(1)', ...)` is known to be
-- non-deterministic only after it is resolved. The seeded forms are deterministic and are cached: each query
-- runs twice, and only the second run of a seeded form is a cache hit.

-- The cache key includes the current database, so this test (running in its own database) does not
-- need to clear the server-wide query cache.

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt64) ENGINE = Memory;
INSERT INTO t VALUES (0), (1);

SET use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;

SELECT 'unseeded table filter', x FROM t FORMAT Null SETTINGS additional_table_filters = {'t': 'x = arrayReduce(\'groupArraySample(1)\', range(2))[1]'};
SELECT 'unseeded table filter', x FROM t FORMAT Null SETTINGS additional_table_filters = {'t': 'x = arrayReduce(\'groupArraySample(1)\', range(2))[1]'};
SELECT 'unseeded result filter', x FROM t FORMAT Null SETTINGS additional_result_filter = 'x = arrayReduce(\'groupArraySample(1)\', range(2))[1]';
SELECT 'unseeded result filter', x FROM t FORMAT Null SETTINGS additional_result_filter = 'x = arrayReduce(\'groupArraySample(1)\', range(2))[1]';

SELECT 'seeded table filter', x FROM t FORMAT Null SETTINGS additional_table_filters = {'t': 'x = arrayReduce(\'groupArraySample(1, 42)\', range(2))[1]'};
SELECT 'seeded table filter', x FROM t FORMAT Null SETTINGS additional_table_filters = {'t': 'x = arrayReduce(\'groupArraySample(1, 42)\', range(2))[1]'};
SELECT 'seeded result filter', x FROM t FORMAT Null SETTINGS additional_result_filter = 'x = arrayReduce(\'groupArraySample(1, 42)\', range(2))[1]';
SELECT 'seeded result filter', x FROM t FORMAT Null SETTINGS additional_result_filter = 'x = arrayReduce(\'groupArraySample(1, 42)\', range(2))[1]';

SET use_query_cache = 0;

SYSTEM FLUSH LOGS query_log;

SELECT extract(query, '^SELECT \'([a-z ]+)\''), result_rows, ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND query LIKE 'SELECT \'%seeded%'
ORDER BY event_time_microseconds;

DROP TABLE t;
