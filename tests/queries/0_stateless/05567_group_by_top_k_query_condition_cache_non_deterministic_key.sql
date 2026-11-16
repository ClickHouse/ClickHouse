-- Tags: no-parallel-replicas
-- no-parallel-replicas: the dynamic filter links the aggregation to the local reading step.

-- A grouping key of `GROUP BY ... LIMIT n` computed by a function that is deterministic only within one query
-- (`joinGet` over a refilled `Join` table, `dictGet` after a reload) changes the number of groups per value of the
-- first key, and so the top-K boundary. The granules emptied by the boundary of one query must not be reused from
-- the query condition cache by the next one.

SET serialize_query_plan = 0;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET max_rows_to_read = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_group_by_top_k_optimization = 1;
SET enable_group_by_top_k_dynamic_filtering = 1;
SET use_top_k_dynamic_filtering = 1;
SET optimize_aggregation_in_order = 0;
SET optimize_trivial_group_by_limit_query = 0;
SET optimize_read_in_order = 0;
SET optimize_move_to_prewhere = 0;
SET use_query_condition_cache = 1;
SET use_query_condition_cache_for_top_k = 1;
SET max_threads = 1;
SET max_block_size = 64;
-- The hash table size hint recorded by the first run must not turn the top-K heap off for the second one.
SET collect_hash_table_stats_during_aggregation = 0;

DROP DICTIONARY IF EXISTS d_group_by_top_k_qcc_key;
DROP TABLE IF EXISTS t_group_by_top_k_qcc_key;
DROP TABLE IF EXISTS w_group_by_top_k_qcc_key;
DROP TABLE IF EXISTS td_group_by_top_k_qcc_key;
DROP TABLE IF EXISTS j_group_by_top_k_qcc_key;
DROP TABLE IF EXISTS src_group_by_top_k_qcc_key;

CREATE TABLE t_group_by_top_k_qcc_key (a UInt64, b UInt64, s String) ENGINE = MergeTree
ORDER BY a SETTINGS index_granularity = 64;
INSERT INTO t_group_by_top_k_qcc_key SELECT 1 + intDiv(number, 64), number % 64, toString(number) FROM numbers(2048);

CREATE TABLE w_group_by_top_k_qcc_key (a UInt64, b UInt64, w UInt8) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 64;
-- Granule 1 (rows 64-127) holds `a = 1` rows that fail `w = 1` and `a = 2` rows beyond the first run's boundary.
INSERT INTO w_group_by_top_k_qcc_key SELECT 1 + intDiv(number, 96), number % 32, number < 64 OR number >= 96 FROM numbers(384);

CREATE TABLE j_group_by_top_k_qcc_key (b UInt64, g UInt64) ENGINE = Join(ANY, LEFT, b);
INSERT INTO j_group_by_top_k_qcc_key SELECT number, number FROM numbers(64);

CREATE TABLE td_group_by_top_k_qcc_key (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 64;
-- A merge would store the defaults computed with the old `Join` contents.
SYSTEM STOP MERGES td_group_by_top_k_qcc_key;
INSERT INTO td_group_by_top_k_qcc_key SELECT 1 + intDiv(number, 64), number % 64 FROM numbers(2048);
-- The part lacks `g`, `h` and `r`: the reader computes them from their defaults.
SET allow_deprecated_error_prone_window_functions = 1;
ALTER TABLE td_group_by_top_k_qcc_key
    ADD COLUMN g UInt64 DEFAULT joinGet({CLICKHOUSE_DATABASE:String} || '.j_group_by_top_k_qcc_key', 'g', b),
    ADD COLUMN h UInt64 DEFAULT b % 3,
    ADD COLUMN r Int64 DEFAULT runningDifference(b);
SET allow_deprecated_error_prone_window_functions = 0;

CREATE TABLE src_group_by_top_k_qcc_key (b UInt64, g UInt64) ENGINE = MergeTree ORDER BY b;
INSERT INTO src_group_by_top_k_qcc_key SELECT number, number FROM numbers(64);
CREATE DICTIONARY d_group_by_top_k_qcc_key (b UInt64, g UInt64) PRIMARY KEY b
SOURCE(CLICKHOUSE(TABLE 'src_group_by_top_k_qcc_key')) LAYOUT(FLAT()) LIFETIME(0);

-- With `g = b`, `a = 1` holds 64 groups, so the boundary is `a = 1`, and the granules with `a > 1` are emptied.
SELECT a, joinGet('j_group_by_top_k_qcc_key', 'g', b) AS g, count() FROM t_group_by_top_k_qcc_key
GROUP BY a, g ORDER BY a, g LIMIT 2;
-- The same under a WHERE filter, whose own cache entry is salted with the top-K boundary.
SELECT a, dictGet('d_group_by_top_k_qcc_key', 'g', b) AS g, count() FROM t_group_by_top_k_qcc_key WHERE s != 'x'
GROUP BY a, g ORDER BY a, g LIMIT 2;
-- The key computed inside the WHERE filter step: `query_plan_split_filter = 0` keeps it there.
SELECT a, g, count() FROM (SELECT a, dictGet('d_group_by_top_k_qcc_key', 'g', b) AS g, s FROM t_group_by_top_k_qcc_key)
WHERE s != 'y' GROUP BY a, g ORDER BY a, g LIMIT 2 SETTINGS query_plan_split_filter = 0;
SELECT a, joinGet('j_group_by_top_k_qcc_key', 'g', b) AS g, count() FROM w_group_by_top_k_qcc_key WHERE w = 1
GROUP BY a, g ORDER BY a, g LIMIT 2;
SELECT a, g, count() FROM td_group_by_top_k_qcc_key GROUP BY a, g ORDER BY a, g LIMIT 2;

TRUNCATE TABLE j_group_by_top_k_qcc_key;
INSERT INTO j_group_by_top_k_qcc_key SELECT number, 0 FROM numbers(64);
TRUNCATE TABLE src_group_by_top_k_qcc_key;
INSERT INTO src_group_by_top_k_qcc_key SELECT number, 0 FROM numbers(64);
SYSTEM RELOAD DICTIONARY d_group_by_top_k_qcc_key;

-- With `g = 0`, `a = 1` holds a single group, so the granules with `a = 2` are needed.
SELECT a, joinGet('j_group_by_top_k_qcc_key', 'g', b) AS g, count() FROM t_group_by_top_k_qcc_key
GROUP BY a, g ORDER BY a, g LIMIT 2 SETTINGS log_comment = '05567_join_key';
SELECT a, dictGet('d_group_by_top_k_qcc_key', 'g', b) AS g, count() FROM t_group_by_top_k_qcc_key WHERE s != 'x'
GROUP BY a, g ORDER BY a, g LIMIT 2;
SELECT a, g, count() FROM (SELECT a, dictGet('d_group_by_top_k_qcc_key', 'g', b) AS g, s FROM t_group_by_top_k_qcc_key)
WHERE s != 'y' GROUP BY a, g ORDER BY a, g LIMIT 2 SETTINGS query_plan_split_filter = 0;
SELECT a, joinGet('j_group_by_top_k_qcc_key', 'g', b) AS g, count() FROM w_group_by_top_k_qcc_key WHERE w = 1
GROUP BY a, g ORDER BY a, g LIMIT 2;
SELECT a, g, count() FROM td_group_by_top_k_qcc_key GROUP BY a, g ORDER BY a, g LIMIT 2
SETTINGS log_comment = '05567_default_key';

-- A deterministic computed key still reuses the cache entries.
SELECT a, b % 3 AS g, count() FROM t_group_by_top_k_qcc_key GROUP BY a, g ORDER BY a, g LIMIT 2;
SELECT a, b % 3 AS g, count() FROM t_group_by_top_k_qcc_key GROUP BY a, g ORDER BY a, g LIMIT 2
SETTINGS log_comment = '05567_deterministic_key';
-- The reader evaluates a default with its own settings, also one this query's settings reject.
SELECT a, count(), max(r) >= 0 FROM td_group_by_top_k_qcc_key GROUP BY a ORDER BY a LIMIT 1;
-- A default that `MODIFY COLUMN` changes changes the boundary too.
SELECT a, h, count() FROM td_group_by_top_k_qcc_key GROUP BY a, h ORDER BY a, h LIMIT 2;
ALTER TABLE td_group_by_top_k_qcc_key MODIFY COLUMN h UInt64 DEFAULT 0
SETTINGS mutations_sync = 0, allow_deprecated_error_prone_window_functions = 1;
SELECT a, h, count() FROM td_group_by_top_k_qcc_key GROUP BY a, h ORDER BY a, h LIMIT 2;
-- A deterministic default keeps the cache: the next run reuses the entries of this one.
SELECT a, h, count() FROM td_group_by_top_k_qcc_key GROUP BY a, h ORDER BY a, h LIMIT 2
SETTINGS log_comment = '05567_default_hit';
-- A virtual column that can change while the part stays the same: no entry is written or reused.
SELECT count() FROM (SELECT a FROM t_group_by_top_k_qcc_key GROUP BY a, _disk_name ORDER BY a, _disk_name LIMIT 2);
SELECT count() FROM (SELECT a FROM t_group_by_top_k_qcc_key GROUP BY a, _disk_name ORDER BY a, _disk_name LIMIT 2)
SETTINGS log_comment = '05567_virtual_key';

SYSTEM FLUSH LOGS query_log;
SET max_block_size = DEFAULT, max_threads = DEFAULT;

-- The dynamic filter still skips rows under the non-deterministic key.
SELECT ProfileEvents['RowsReadByMainReader'] < 2048 FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND log_comment = '05567_join_key';
-- Also under a non-deterministic default.
SELECT ProfileEvents['RowsReadByMainReader'] < 2048 FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND log_comment = '05567_default_key';
-- The deterministic key hits the cache.
SELECT ProfileEvents['QueryConditionCacheHits'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND log_comment = '05567_deterministic_key';
-- So does the deterministic default.
SELECT ProfileEvents['QueryConditionCacheHits'] > 0 FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND log_comment = '05567_default_hit';
-- The virtual key does not, and the dynamic filter still skips rows under it.
SELECT ProfileEvents['QueryConditionCacheHits'] = 0, ProfileEvents['RowsReadByMainReader'] < 2048 FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND log_comment = '05567_virtual_key';

DROP DICTIONARY d_group_by_top_k_qcc_key;
DROP TABLE t_group_by_top_k_qcc_key;
DROP TABLE w_group_by_top_k_qcc_key;
DROP TABLE td_group_by_top_k_qcc_key;
DROP TABLE j_group_by_top_k_qcc_key;
DROP TABLE src_group_by_top_k_qcc_key;
