-- Regression test for cases 2, 5, 6 and 7 of https://github.com/ClickHouse/ClickHouse/issues/116705: top-k optimizations over a float sort column containing `nan`.
-- Every `on` arm must return the same rows as its `off` arm, which runs with every top-k optimization disabled.

SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET use_query_condition_cache = 0;
SET max_threads = 1;
SET max_block_size = 1000;

DROP TABLE IF EXISTS t_topk_nan_mixed;
DROP TABLE IF EXISTS t_topk_nan_late;
DROP TABLE IF EXISTS t_topk_nan_threshold;

-- Case 2: skip-index top-k under `DESC NULLS FIRST`. The `minmax` index leaves the single `nan` out of the bounds of the first granule, whose finite max (63) is the smallest of all granules, so ranking granules by their max drops the granule holding the `nan`.
CREATE TABLE t_topk_nan_mixed (id UInt32, f64 Float64, INDEX f64_idx f64 TYPE minmax)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, use_const_adaptive_granularity = 1, index_granularity_bytes = 0;
INSERT INTO t_topk_nan_mixed SELECT number, if(number = 5, nan, toFloat64(number)) FROM numbers(10000);

SELECT 'c2 index', f64 FROM t_topk_nan_mixed ORDER BY f64 DESC NULLS FIRST LIMIT 2
SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 0, use_skip_indexes_on_data_read = 0;
SELECT 'c2 all', f64 FROM t_topk_nan_mixed ORDER BY f64 DESC NULLS FIRST LIMIT 2
SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 1, use_skip_indexes_on_data_read = 1;
SELECT 'c2 off', f64 FROM t_topk_nan_mixed ORDER BY f64 DESC NULLS FIRST LIMIT 2
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 0;

-- Cases 5 and 6: dynamic filtering under `NULLS FIRST`. The four `nan` rows are read long after the first block has published a finite threshold, and they must still pass the filter because `nan` sorts before every finite value.
CREATE TABLE t_topk_nan_late (id UInt32, f64 Float64, f32 Float32)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_topk_nan_late SELECT number, if(number IN (8000, 8500, 9000, 9500), nan, toFloat64(number)), toFloat32(if(number IN (8000, 8500, 9000, 9500), nan, toFloat64(number))) FROM numbers(10000);

SELECT 'c5 f64 on', f64 FROM t_topk_nan_late ORDER BY f64 ASC NULLS FIRST LIMIT 6
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 1;
SELECT 'c5 f64 off', f64 FROM t_topk_nan_late ORDER BY f64 ASC NULLS FIRST LIMIT 6
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 0;
SELECT 'c5 f32 on', f32 FROM t_topk_nan_late ORDER BY f32 ASC NULLS FIRST LIMIT 6
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 1;
SELECT 'c5 f32 off', f32 FROM t_topk_nan_late ORDER BY f32 ASC NULLS FIRST LIMIT 6
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 0;

SELECT 'c6 f64 on', f64 FROM t_topk_nan_late ORDER BY f64 DESC NULLS FIRST LIMIT 6
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 1;
SELECT 'c6 f64 off', f64 FROM t_topk_nan_late ORDER BY f64 DESC NULLS FIRST LIMIT 6
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 0;
SELECT 'c6 f32 on', f32 FROM t_topk_nan_late ORDER BY f32 DESC NULLS FIRST LIMIT 6
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 1;
SELECT 'c6 f32 off', f32 FROM t_topk_nan_late ORDER BY f32 DESC NULLS FIRST LIMIT 6
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 0;

-- Case 7: dynamic filtering under the default `NULLS LAST`. The first block holds two finite rows and 998 `nan`, so the row at position `LIMIT - 1` of its sorted top-k is `nan`; a `nan` threshold must not reject the better finite rows of the later blocks.
CREATE TABLE t_topk_nan_threshold (id UInt32, f64 Float64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_topk_nan_threshold SELECT number, multiIf(number = 0, 1.0, number = 1, 2.0, number < 1000, nan, 0.0001 * (number - 999)) FROM numbers(10000);

SELECT 'c7 asc on', f64 FROM t_topk_nan_threshold ORDER BY f64 ASC LIMIT 3
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 1;
SELECT 'c7 asc off', f64 FROM t_topk_nan_threshold ORDER BY f64 ASC LIMIT 3
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 0;
SELECT 'c7 desc on', f64 FROM t_topk_nan_threshold ORDER BY f64 DESC LIMIT 3
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 1;
SELECT 'c7 desc off', f64 FROM t_topk_nan_threshold ORDER BY f64 DESC LIMIT 3
SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 0;

DROP TABLE t_topk_nan_mixed;
DROP TABLE t_topk_nan_late;
DROP TABLE t_topk_nan_threshold;
