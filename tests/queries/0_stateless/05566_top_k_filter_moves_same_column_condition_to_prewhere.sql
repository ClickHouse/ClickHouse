-- Tags: no-parallel
-- Tag no-parallel: the repeated query of the cache arm reads the entry the first one wrote to the server-wide query condition cache, which other tests clear

-- A WHERE condition on the only column of `ORDER BY ... LIMIT` is moved to PREWHERE next to `__topKFilter`,
-- which reads that column anyway, so PREWHERE records the granules the condition rejects in the query condition cache.

-- optimize_empty_string_comparisons turns s != '' into the notEmpty the plan arm looks for.
SET optimize_move_to_prewhere = 1, enable_multiple_prewhere_read_steps = 1, optimize_empty_string_comparisons = 1;
SET use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 1000;
SET use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1;
-- One part (max_insert_threads = 1), one thread, 256-row blocks and no range-split injection (it can make the read
-- in order) keep the granule counts of the cache arm exact.
SET max_threads = 1, max_block_size = 256;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS t_top_k_prewhere;

CREATE TABLE t_top_k_prewhere (id UInt32, s String, n UInt32)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0, add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_string_columns = 0;

-- 100 granules: every 4th one holds a single non-empty value, decreasing along the part, the others only empty strings.
INSERT INTO t_top_k_prewhere
SELECT number, if(number % 256 = 0, concat('v', leftPad(toString(1000000 - intDiv(number, 64)), 7, '0')), ''), number % 10
FROM numbers(6400) SETTINGS max_insert_threads = 1;

SELECT 'plan';
SELECT countIf(explain LIKE '%Prewhere filter column%__topKFilter%' AND explain LIKE '%Prewhere filter column%notEmpty%'),
    countIf(explain LIKE '%Filter column: notEmpty%')
FROM (EXPLAIN actions = 1 SELECT s FROM t_top_k_prewhere WHERE s != '' ORDER BY s LIMIT 5);
SELECT countIf(explain LIKE '%Prewhere filter column%'), countIf(explain LIKE '%Filter column: notEmpty%')
FROM (EXPLAIN actions = 1 SELECT s FROM t_top_k_prewhere WHERE s != '' ORDER BY s LIMIT 5 SETTINGS use_top_k_dynamic_filtering = 0);
-- A lazily executed condition stays in WHERE.
SELECT countIf(explain LIKE '%Prewhere filter column%__topKFilter%' AND explain NOT LIKE '%Prewhere filter column%LIKE%'),
    countIf(explain LIKE '%Filter column: s LIKE%')
FROM (EXPLAIN actions = 1 SELECT s FROM t_top_k_prewhere WHERE s LIKE '%v1%' ORDER BY s LIMIT 5);
-- The same for a number column.
SELECT countIf(explain LIKE '%Prewhere filter column%__topKFilter%' AND explain LIKE '%Prewhere filter column%n > 3%'),
    countIf(explain LIKE '%Filter column: n > 3%')
FROM (EXPLAIN actions = 1 SELECT n FROM t_top_k_prewhere WHERE n > 3 ORDER BY n LIMIT 5);

SELECT 'results';
SELECT s FROM t_top_k_prewhere WHERE s != '' ORDER BY s LIMIT 5 SETTINGS use_query_condition_cache = 0, use_top_k_dynamic_filtering = 0;
SELECT s FROM t_top_k_prewhere WHERE s != '' ORDER BY s LIMIT 5 SETTINGS log_comment = '05566_first';
SELECT s FROM t_top_k_prewhere WHERE s != '' ORDER BY s LIMIT 5 SETTINGS log_comment = '05566_repeated';
SELECT s FROM t_top_k_prewhere WHERE s != '' ORDER BY s DESC LIMIT 3 SETTINGS use_query_condition_cache = 0, use_top_k_dynamic_filtering = 0;
SELECT s FROM t_top_k_prewhere WHERE s != '' ORDER BY s DESC LIMIT 3;
SELECT s FROM t_top_k_prewhere WHERE s != '' ORDER BY s DESC LIMIT 3;
-- A condition that passes every row.
SELECT s FROM t_top_k_prewhere WHERE s != 'zzzz' ORDER BY s DESC LIMIT 3;
SELECT s FROM t_top_k_prewhere WHERE s != 'zzzz' ORDER BY s DESC LIMIT 3;

SELECT 'cache';
SYSTEM FLUSH LOGS query_log;
SET max_threads = 0, max_block_size = 65409;
SELECT
    (SELECT max(ProfileEvents['SelectedMarks']) FROM system.query_log
     WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05566_repeated')
    < (SELECT max(ProfileEvents['SelectedMarks']) FROM system.query_log
       WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05566_first');

-- The guard runs first: the conversion stays in WHERE, after the condition moved to PREWHERE.
SELECT 'guard';
SELECT s FROM t_top_k_prewhere WHERE s != '' AND toUInt64(substring(s, 2)) % 8 = 0 ORDER BY s LIMIT 3;

DROP TABLE t_top_k_prewhere;
