SET allow_experimental_time_decay_aggregate_functions = 1;
SET max_threads = 1;
SET max_block_size = 1;
SET optimize_read_in_order = 0;
SET use_top_k_dynamic_filtering = 1;
SET use_top_k_dynamic_filtering_for_variable_length_types = 1;
SET use_query_condition_cache = 0;
SET enable_parallel_replicas = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;

CREATE TABLE time_decay_topk64
(
    k Tuple(ExponentialTimeDecaying64(10), UInt8),
    source_time UInt64
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO time_decay_topk64
SELECT tuple(CAST((-1., toFloat64(number * 100)), 'ExponentialTimeDecaying64(10)'), toUInt8(0)), number * 100
FROM numbers(11);

-- Physical order runs from timestamp 1000 to 0. Filter out the final row, which cannot
-- be pruned using a following mark, so a wrong threshold cannot accidentally find it.
SELECT source_time FROM time_decay_topk64 WHERE source_time > 0 ORDER BY k DESC LIMIT 1
SETTINGS use_skip_indexes_on_data_read = 0;
SELECT source_time FROM time_decay_topk64 WHERE source_time > 0 ORDER BY k DESC LIMIT 1
SETTINGS use_skip_indexes_on_data_read = 1;

CREATE TABLE time_decay_topk128
(
    k Tuple(ExponentialTimeDecaying128(10, direct), UInt8),
    source_time UInt64
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1, index_granularity_bytes = 0,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO time_decay_topk128
SELECT tuple(CAST((-1., toFloat64(number * 100)), 'ExponentialTimeDecaying128(10, direct)'), toUInt8(0)), number * 100
FROM numbers(11);

SELECT source_time FROM time_decay_topk128 WHERE source_time > 0 ORDER BY k DESC LIMIT 1
SETTINGS use_skip_indexes_on_data_read = 0;
SELECT source_time FROM time_decay_topk128 WHERE source_time > 0 ORDER BY k DESC LIMIT 1
SETTINGS use_skip_indexes_on_data_read = 1;

DROP TABLE time_decay_topk64;
DROP TABLE time_decay_topk128;
