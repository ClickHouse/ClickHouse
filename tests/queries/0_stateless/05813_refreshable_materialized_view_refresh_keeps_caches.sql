-- Tags: no-parallel, no-ordinary-database, no-replicated-database
-- Tag no-parallel: checks mark cache hits, which other tests can drop
-- Tag no-ordinary-database: a non-APPEND refreshable materialized view needs an Atomic database
-- Tag no-replicated-database: a Replicated database refuses a non-APPEND refreshable materialized view over a plain MergeTree

-- A refresh of a refreshable materialized view must not drop the server caches.

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS t;

CREATE TABLE t (a UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS prewarm_mark_cache = 0;
INSERT INTO t SELECT number FROM numbers(1000);
CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 YEAR (x UInt64) ENGINE = MergeTree ORDER BY x EMPTY AS SELECT 1 AS x;

SET use_query_condition_cache = 0, use_columns_cache = 0, load_marks_asynchronously = 0, enable_parallel_replicas = 0;

SELECT count() FROM t WHERE a < 50 FORMAT Null SETTINGS log_comment = '05813_cold';
SELECT count() FROM t WHERE a < 50 FORMAT Null SETTINGS log_comment = '05813_warm';
SYSTEM REFRESH VIEW mv;
SYSTEM WAIT VIEW mv;
SELECT count() FROM t WHERE a < 50 FORMAT Null SETTINGS log_comment = '05813_after_refresh';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['MarkCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05813_%'
ORDER BY event_time_microseconds;

DROP TABLE mv;
DROP TABLE t;
