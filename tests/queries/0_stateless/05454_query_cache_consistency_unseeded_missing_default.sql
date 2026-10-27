-- `query_cache_use_only_when_data_was_not_changed` must not cache a result that can change while every
-- referenced table stays the same. A column added by `ALTER TABLE ... ADD COLUMN` is missing in the old
-- part, so the read evaluates its `DEFAULT` expression. `arrayReduce('groupArraySample(1)', ...)` is not
-- deterministic only after the aggregate function is resolved. The seeded form is deterministic and is
-- cached: each query runs twice, and only the second run of the seeded form is a cache hit.

-- The cache key includes the current database, so this test (running in its own database) does not
-- need to clear the server-wide query cache.

DROP TABLE IF EXISTS t_unseeded;
DROP TABLE IF EXISTS t_seeded;

CREATE TABLE t_unseeded (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_unseeded VALUES (1);
ALTER TABLE t_unseeded ADD COLUMN y Array(UInt64) DEFAULT arrayReduce('groupArraySample(1)', range(1000));

CREATE TABLE t_seeded (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_seeded VALUES (1);
ALTER TABLE t_seeded ADD COLUMN y Array(UInt64) DEFAULT arrayReduce('groupArraySample(1, 42)', range(1000));

SET use_query_cache = 1, query_cache_min_query_runs = 0, query_cache_min_query_duration = 0, query_cache_use_only_when_data_was_not_changed = 1;

SELECT 'unseeded', length(y) FROM t_unseeded;
SELECT 'unseeded', length(y) FROM t_unseeded;
SELECT 'seeded', length(y) FROM t_seeded;
SELECT 'seeded', length(y) FROM t_seeded;

SET use_query_cache = 0;

SYSTEM FLUSH LOGS query_log;

SELECT extract(query, '^SELECT \'([a-z]+)\''), ProfileEvents['QueryCacheHits']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND query LIKE 'SELECT \'%seeded%'
ORDER BY event_time_microseconds;

DROP TABLE t_unseeded;
DROP TABLE t_seeded;
