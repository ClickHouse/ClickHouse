-- The maximum values of `merge_tree_max_rows_to_use_cache` and `merge_tree_max_bytes_to_use_cache` mean "always use the cache".
-- Rounding them up to marks wrapped around to 0, which disabled the uncompressed cache for every query.

DROP TABLE IF EXISTS t_max_rows_to_use_cache;
CREATE TABLE t_max_rows_to_use_cache (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO t_max_rows_to_use_cache SELECT number FROM numbers(100000);

SELECT sum(x) FROM t_max_rows_to_use_cache
SETTINGS use_uncompressed_cache = 1, use_columns_cache = 0, merge_tree_max_rows_to_use_cache = 18446744073709551615, merge_tree_max_bytes_to_use_cache = 18446744073709551615, log_comment = '05407_max_value';

SELECT sum(x) FROM t_max_rows_to_use_cache
SETTINGS use_uncompressed_cache = 1, use_columns_cache = 0, merge_tree_max_rows_to_use_cache = 1000000000000, merge_tree_max_bytes_to_use_cache = 1000000000000, log_comment = '05407_large_value';

SYSTEM FLUSH LOGS query_log;

-- With parallel replicas the reads can run on other replicas, whose events are in their own rows of the same initial query.
SELECT log_comment, sum(ProfileEvents['UncompressedCacheHits'] + ProfileEvents['UncompressedCacheMisses']) > 0
FROM system.query_log
WHERE type = 'QueryFinish' AND initial_query_id IN
(
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query AND log_comment IN ('05407_max_value', '05407_large_value')
)
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_max_rows_to_use_cache;
