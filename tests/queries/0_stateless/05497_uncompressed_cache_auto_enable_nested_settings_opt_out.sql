-- Tags: no-random-settings, no-object-storage
-- Tag no-random-settings: the test asserts uncompressed cache profile events, which a randomized
-- `use_uncompressed_cache` would distort.
-- Tag no-object-storage: automatic mode never applies to parts on object storage.

-- An explicit `use_uncompressed_cache = 0` in a nested `SETTINGS` clause (a subquery, the stored query of a view)
-- must win over `enable_automatic_use_uncompressed_cache = 1`, even though its value equals the default.

DROP TABLE IF EXISTS t_uncompressed_cache_nested;
DROP VIEW IF EXISTS v_uncompressed_cache_nested;

CREATE TABLE t_uncompressed_cache_nested
(
    id UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

INSERT INTO t_uncompressed_cache_nested SELECT number, repeat('x', 128) FROM numbers(32768);

CREATE VIEW v_uncompressed_cache_nested AS SELECT payload FROM t_uncompressed_cache_nested SETTINGS use_uncompressed_cache = 0;

SET log_queries = 1;
SET max_threads = 1;
SET use_query_condition_cache = 0;

-- Control: a subquery without the opt-out uses the cache.
SELECT sum(length(payload)) = 32768 * 128 FROM (SELECT payload FROM t_uncompressed_cache_nested)
SETTINGS enable_automatic_use_uncompressed_cache = 1, log_comment = '05497_control';

SELECT sum(length(payload)) = 32768 * 128 FROM (SELECT payload FROM t_uncompressed_cache_nested SETTINGS use_uncompressed_cache = 0)
SETTINGS enable_automatic_use_uncompressed_cache = 1, log_comment = '05497_subquery_opt_out';

SELECT sum(length(payload)) = 32768 * 128 FROM v_uncompressed_cache_nested
SETTINGS enable_automatic_use_uncompressed_cache = 1, log_comment = '05497_view_opt_out';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND current_database = currentDatabase()
  AND log_comment IN ('05497_control', '05497_subquery_opt_out', '05497_view_opt_out')
ORDER BY log_comment;

DROP VIEW v_uncompressed_cache_nested;
DROP TABLE t_uncompressed_cache_nested;
