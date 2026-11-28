-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings
-- no-parallel: the hit and miss counts below depend on the server-wide columns cache, which
-- `SYSTEM DROP COLUMNS CACHE` and the admission control of the cache share with other tests.

-- The columns cache holds the columns of projection parts. Every part has its own part of the
-- projection, all named after the projection, so the entries of a projection part are keyed by
-- the name of its parent part and the projection: the parts of two parents do not collide.

SET max_threads = 1;
SET enable_parallel_replicas = 0;
SET optimize_use_projections = 1;

DROP TABLE IF EXISTS t_cc_proj;

CREATE TABLE t_cc_proj
(
    id UInt64,
    k UInt64,
    s String,
    PROJECTION by_k (SELECT k, id, s ORDER BY k),
    PROJECTION agg (SELECT k % 10, count(), sum(id) GROUP BY k % 10)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

SYSTEM STOP MERGES t_cc_proj;

-- Two parts with different data, each with its own parts of the projections.
INSERT INTO t_cc_proj SELECT number, number % 1000, toString(number) FROM numbers(100000);
INSERT INTO t_cc_proj SELECT number + 100000, number % 1000 + 1000, repeat(toString(number), 2) FROM numbers(100000);

SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_cc_proj' AND active;

SYSTEM DROP COLUMNS CACHE;

-- Read from the projection `by_k`: the condition on `k` is on its sorting key.
SELECT count(), sum(id), sum(length(s)) FROM t_cc_proj WHERE k BETWEEN 100 AND 1100
SETTINGS use_columns_cache = 1, force_optimize_projection = 1, log_comment = 'cc_proj_write';

-- The entries of each projection part are under the name of its parent part.
SELECT replaceRegexpOne(part, '^[^:]+:', 'parent:'), column, count() > 0
FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_proj'
GROUP BY 1, 2 ORDER BY 1, 2;

SELECT uniqExact(part) = 2,
    countIf(part NOT IN (SELECT name || ':by_k' FROM system.parts WHERE database = currentDatabase() AND table = 't_cc_proj' AND active)) = 0
FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_proj';

-- The same query is served from the cache, with the same result.
SELECT count(), sum(id), sum(length(s)) FROM t_cc_proj WHERE k BETWEEN 100 AND 1100
SETTINGS use_columns_cache = 1, force_optimize_projection = 1, log_comment = 'cc_proj_hit';

SELECT count(), sum(id), sum(length(s)) FROM t_cc_proj WHERE k BETWEEN 100 AND 1100
SETTINGS use_columns_cache = 0, force_optimize_projection = 1;

-- An aggregate projection.
SELECT k % 10 AS r, count(), sum(id) FROM t_cc_proj GROUP BY r ORDER BY r SETTINGS use_columns_cache = 1, force_optimize_projection = 1;
SELECT k % 10 AS r, count(), sum(id) FROM t_cc_proj GROUP BY r ORDER BY r SETTINGS use_columns_cache = 1, force_optimize_projection = 1;
SELECT k % 10 AS r, count(), sum(id) FROM t_cc_proj GROUP BY r ORDER BY r SETTINGS use_columns_cache = 0, force_optimize_projection = 1;

SELECT uniqExact(part) FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_proj' AND part LIKE '%:agg';

-- The parent parts are cached under their own names, next to their projections.
SELECT sum(id), sum(length(s)) FROM t_cc_proj SETTINGS use_columns_cache = 1, optimize_use_projections = 0;
SELECT countIf(part NOT LIKE '%:%') > 0 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cc_proj';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['ColumnsCacheHits'] > 0 AS hits,
    ProfileEvents['ColumnsCacheMisses'] AS misses
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'cc_proj_%'
ORDER BY event_time_microseconds;

DROP TABLE t_cc_proj;
