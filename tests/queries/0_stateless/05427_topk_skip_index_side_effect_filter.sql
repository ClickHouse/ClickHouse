-- A condition with observable side effects (`sleepEachRow`) must run on every candidate row, so the read-time
-- skip-index top-K pruning must not skip granules below it either. This includes the `ALIAS` column shape, where
-- the `ALIAS` expression is lifted above the `Sort`.
-- `max_rows_to_read` must be reset: a non-zero value with `read_overflow_mode = 'throw'` disables the on-data-read
-- skip-index path (see `ReadFromMergeTree::supportsSkipIndexesOnDataRead`).

SET enable_parallel_replicas = 0;
SET use_skip_indexes = 1, use_skip_indexes_for_top_k = 1, use_skip_indexes_on_data_read = 1;
SET use_top_k_dynamic_filtering = 0, query_plan_max_limit_for_top_k_optimization = 1000;
SET max_threads = 1, max_block_size = 128, max_rows_to_read = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS t_topk_skip_index_side_effect;
CREATE TABLE t_topk_skip_index_side_effect
(
    time DateTime64(3),
    body String,
    body_alias String ALIAS concat(body, '!'),
    INDEX time_minmax(time) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100;

INSERT INTO t_topk_skip_index_side_effect
SELECT toDateTime64('2020-01-01 00:00:00', 3) - number, toString(number) FROM numbers(10000);

-- Without side effects the granules beyond the top-K threshold are skipped.
SELECT body_alias FROM t_topk_skip_index_side_effect WHERE body != '' ORDER BY time DESC LIMIT 3
SETTINGS log_comment = '05427_no_side_effect';

-- With `sleepEachRow` in the condition every row reaches it.
SELECT body_alias FROM t_topk_skip_index_side_effect WHERE sleepEachRow(0) = 0 ORDER BY time DESC LIMIT 3
SETTINGS log_comment = '05427_where_side_effect';
SELECT time FROM t_topk_skip_index_side_effect WHERE sleepEachRow(0) = 0 ORDER BY time DESC LIMIT 3
SETTINGS log_comment = '05427_where_side_effect_physical';
SELECT body_alias FROM t_topk_skip_index_side_effect PREWHERE sleepEachRow(0) = 0 ORDER BY time DESC LIMIT 3
SETTINGS log_comment = '05427_prewhere_side_effect';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, read_rows < 5000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05427_no_side_effect', '05427_where_side_effect', '05427_where_side_effect_physical', '05427_prewhere_side_effect')
ORDER BY log_comment;

DROP TABLE t_topk_skip_index_side_effect;
