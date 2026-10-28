-- The query condition cache must not record a verdict for `ORDER BY ... LIMIT` computed from a non-deterministic default.
-- Every case prints the result with the cache and without it.

SET use_query_condition_cache = 1;
-- Without a local plan the filter steps run as part of the remote queries, and this server's cache sees nothing.
SET parallel_replicas_local_plan = 1;
-- The pending mutations below must not run, and the `ALTER`s must not wait for them.
SET mutations_sync = 0;
SET alter_sync = 0;
SET session_timezone = 'UTC';
-- WHERE cases must stay WHERE: the automatic move to PREWHERE would exercise the other writer.
SET optimize_move_to_prewhere = 0;

-- The auto minmax indexes would answer before the cache, and the cache stores nothing for small parts.
-- ORDER BY ... LIMIT: the part without `v` sets the threshold the granules of the other part are dropped by.
CREATE TABLE t_now_top_k (k UInt64, tag UInt8, keep UInt8) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_now_top_k;
INSERT INTO t_now_top_k SELECT number, 0, 1 FROM numbers(100000);
ALTER TABLE t_now_top_k ADD COLUMN v DateTime64(3) DEFAULT now64(3) + toIntervalMillisecond(k);
INSERT INTO t_now_top_k SELECT number, 1, 1, now64(3) + 5 FROM numbers(100000);
SELECT tag FROM t_now_top_k WHERE keep = 1 ORDER BY v LIMIT 1
    SETTINGS use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, max_threads = 1
    FORMAT Null;

SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;
SELECT 'DEFAULT now64, ORDER BY LIMIT', tag FROM t_now_top_k WHERE keep = 1 ORDER BY v LIMIT 1
    SETTINGS use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, max_threads = 1;
SELECT 'DEFAULT now64, ORDER BY LIMIT', tag FROM t_now_top_k WHERE keep = 1 ORDER BY v LIMIT 1
    SETTINGS use_query_condition_cache = 0, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, max_threads = 1;

-- ORDER BY ... LIMIT is still cached when every part stores the column with the non-deterministic default.
CREATE TABLE t_now_top_k_stored (k UInt64, tag UInt8, keep UInt8, v DateTime64(3) DEFAULT now64(3) + toIntervalMillisecond(k)) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_now_top_k_stored;
INSERT INTO t_now_top_k_stored (k, tag, keep) SELECT number, 0, 1 FROM numbers(100000);
INSERT INTO t_now_top_k_stored SELECT number, 1, 1, now64(3) + 5 FROM numbers(100000);
SELECT tag FROM t_now_top_k_stored WHERE keep = 1 ORDER BY v LIMIT 1
    SETTINGS use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, max_threads = 1
    FORMAT Null;
SELECT tag FROM t_now_top_k_stored WHERE keep = 1 ORDER BY v LIMIT 1
    SETTINGS use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, max_threads = 1,
    log_comment = '05459_top_k_hit' FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['QueryConditionCacheHits'] > 0, ProfileEvents['SelectedMarks'] < ProfileEvents['SelectedMarksTotal']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05459_top_k_hit')
ORDER BY log_comment;
