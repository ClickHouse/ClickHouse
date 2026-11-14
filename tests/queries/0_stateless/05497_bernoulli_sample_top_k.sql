-- Random settings limits: use_skip_indexes_for_top_k=(1, 1); query_plan_max_limit_for_top_k_optimization=(10, None)

-- Bernoulli `SAMPLE` drops rows after the read, so neither the skip-index top-K nor the TopK entries of the query
-- condition cache may narrow the read to the granules that hold the extreme `k` of the whole table: none of their
-- rows may be sampled.

SET allow_experimental_bernoulli_sample = 1;
SET bernoulli_sample_seed = 42;
SET optimize_read_in_order = 0;
SET optimize_use_projections = 0;

DROP TABLE IF EXISTS t_bernoulli_top_k;

CREATE TABLE t_bernoulli_top_k (id UInt32, k UInt32, v UInt32, INDEX mm_k k TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8;

INSERT INTO t_bernoulli_top_k SELECT number, number, number FROM numbers(100000);

SELECT '--- skip-index top-K';
SELECT
    (SELECT groupArray(k) FROM (SELECT k FROM t_bernoulli_top_k SAMPLE 0.001 ORDER BY k LIMIT 3
        SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 0))
    =
    (SELECT groupArray(k) FROM (SELECT k FROM t_bernoulli_top_k SAMPLE 0.001 ORDER BY k LIMIT 3
        SETTINGS use_skip_indexes_for_top_k = 0, use_top_k_dynamic_filtering = 0));
SELECT length(groupArray(k)) FROM (SELECT k FROM t_bernoulli_top_k SAMPLE 0.001 ORDER BY k LIMIT 3
    SETTINGS use_skip_indexes_for_top_k = 1, use_top_k_dynamic_filtering = 0);

SELECT '--- query condition cache with TopK entries of an unsampled read';
-- Warm the cache with the granules the unsampled TopK read excluded.
SELECT k FROM t_bernoulli_top_k WHERE v >= 0 ORDER BY k LIMIT 3 FORMAT Null
    SETTINGS use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1,
        use_skip_indexes_for_top_k = 0, optimize_move_to_prewhere = 0, max_threads = 1;
SELECT
    (SELECT groupArray(k) FROM (SELECT k FROM t_bernoulli_top_k SAMPLE 0.001 WHERE v >= 0 ORDER BY k LIMIT 3
        SETTINGS use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1, use_top_k_dynamic_filtering = 1,
            use_skip_indexes_for_top_k = 0, optimize_move_to_prewhere = 0, max_threads = 1))
    =
    (SELECT groupArray(k) FROM (SELECT k FROM t_bernoulli_top_k SAMPLE 0.001 WHERE v >= 0 ORDER BY k LIMIT 3
        SETTINGS use_query_condition_cache = 0, use_top_k_dynamic_filtering = 0, use_skip_indexes_for_top_k = 0));

DROP TABLE t_bernoulli_top_k;
