-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas, no-ordinary-database
-- With `analyzer_compatibility_apply_final_to_all_joined_tables = 1`, a table without its own modifiers
-- inherits the query-level `FINAL`. The logical read of the query plan cache must keep that effective
-- `FINAL`: both a cache miss and a hit must return the same result as a run without the cache.
-- The plan cache is a single, server-wide cache, so the test runs in isolation (see 04489).

SYSTEM DROP QUERY PLAN CACHE;

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left (id Int64, right_id Int64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY id;
CREATE TABLE t_right (id Int64, attr String, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY id;

SYSTEM STOP MERGES t_left;
SYSTEM STOP MERGES t_right;

-- One logical row per table, two unmerged versions each (separate parts).
INSERT INTO t_left VALUES (1, 10, 1);
INSERT INTO t_left VALUES (1, 10, 2);
INSERT INTO t_right VALUES (10, 'car', 1);
INSERT INTO t_right VALUES (10, 'car', 2);

SET analyzer_compatibility_apply_final_to_all_joined_tables = 1;

SELECT count() FROM t_left AS l FINAL INNER JOIN t_right AS r ON r.id = l.right_id SETTINGS log_comment = '05459_off';

SET allow_experimental_query_plan_cache = 1, enable_query_plan_cache = 1;

SELECT count() FROM t_left AS l FINAL INNER JOIN t_right AS r ON r.id = l.right_id SETTINGS log_comment = '05459_miss';
SELECT count() FROM t_left AS l FINAL INNER JOIN t_right AS r ON r.id = l.right_id SETTINGS log_comment = '05459_hit';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['QueryPlanCacheHits']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05459_off', '05459_miss', '05459_hit')
ORDER BY event_time_microseconds;

DROP TABLE t_left;
DROP TABLE t_right;
