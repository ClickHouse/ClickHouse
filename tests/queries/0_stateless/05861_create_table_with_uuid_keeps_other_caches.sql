-- Tags: no-parallel, no-ordinary-database, no-replicated-database
-- Tag no-parallel: checks mark cache hits, which other tests can drop
-- Tag no-ordinary-database: a table with an explicit UUID needs an Atomic database
-- Tag no-replicated-database: a Replicated database refuses an explicit UUID

-- Creating and dropping a table with an explicit UUID must not drop the caches of other tables.

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_uuid SYNC;

CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS prewarm_mark_cache = 0;
INSERT INTO t SELECT number FROM numbers(1000);

SET use_query_condition_cache = 0, use_columns_cache = 0, load_marks_asynchronously = 0, enable_parallel_replicas = 0;

SELECT count() FROM t WHERE a < 50 FORMAT Null SETTINGS log_comment = '05861_cold';
SELECT count() FROM t WHERE a < 50 FORMAT Null SETTINGS log_comment = '05861_warm';
CREATE TABLE t_uuid UUID 'a9d0c1e2-58f6-4b3a-9c7d-05861e0d1f2a' (a UInt64) ENGINE = MergeTree ORDER BY tuple();
SELECT count() FROM t WHERE a < 50 FORMAT Null SETTINGS log_comment = '05861_after_create';
DROP TABLE t_uuid SYNC;
SELECT count() FROM t WHERE a < 50 FORMAT Null SETTINGS log_comment = '05861_after_drop';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['MarkCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05861_cold', '05861_warm', '05861_after_create', '05861_after_drop')
ORDER BY event_time_microseconds;

DROP TABLE t;
