-- The bounding mode of the multi-pass top-K aggregation of `GROUP BY ... ORDER BY count() DESC LIMIT n` over
-- `MergeTree`. Every query runs with the optimization and without it, which must return the same rows, and
-- the profile events show which passes ran. When the sample cannot show the top and an integer key bounds the
-- groups, the bounding mode runs the sketch pass, the first restricted pass over the values of the key with
-- most rows, and the hot pass over the other values that may hold a group of the top.

-- The passes read blocks of 64 rows, one granule each, whatever streams read them. A query that runs no pass
-- reads blocks of 65536 rows, which leave its result unchanged: the executor takes locks for every block, and
-- the thread fuzzer of the flaky check slows down every lock. The settings that would turn the optimization
-- off or change the shape of the plan are pinned. The injection that splits the ranges of a read is off: the
-- read cannot estimate its bytes with it, so a limit of the bytes to read would keep the optimization off.
SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, enable_adaptive_aggregator = 1, adaptive_aggregator_top_k = 1;
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 64, optimize_aggregation_in_order = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, serialize_query_plan = 0;
SET group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET collect_hash_table_stats_during_aggregation = 0;

DROP TABLE IF EXISTS t_bounded;

-- 67584 rows in a single Wide part of 1056 granules of exactly 64 rows. The optimization needs at least 1024
-- granules, and the pilot samples every 32nd one. The insert writes one part, so no merge changes the
-- granules.
-- `g`, and its copies of the other integer types that a bounding key may have: the value `i` holds `2i + 1`
-- consecutive rows, so the values come in runs. The signed copies hold negative values, and the 64-bit copies
-- hold values beyond 32 bits.
-- `s`: a single value, so the groups of `(g, s)` are the runs of `g`.
-- `t`: splits each run of `g` from the value 200 on into 5 groups, so the values of `g` with most rows hold
-- only small groups.
CREATE TABLE t_bounded
(
    id UInt64,
    g UInt32,
    g_i32 Int32,
    g_i64 Int64,
    g_u64 UInt64,
    s String,
    t String
)
ENGINE = MergeTree ORDER BY (g, id)
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_bounded
SELECT
    number AS id,
    toUInt32(sqrt(number)) AS g,
    -toInt32(g) AS g_i32,
    -toInt64(g) - 5000000000 AS g_i64,
    toUInt64(g) + 0xF000000000000000 AS g_u64,
    'x' AS s,
    if(g >= 200, toString(number % 5), 'x') AS t
FROM numbers(67584)
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

-- The bounding mode by a key of each integer type. Its first restricted pass takes the 20 values of the key
-- with most rows, 4 for every group of the top. `g` is part of the primary key, so the restricted passes read
-- only the granules of its values. Its copies are not part of it.
-- These 20 values hold the 5 best groups of `(g, s)`, and no other value holds as many rows as the fifth best
-- group, so the first restricted pass proves the top. The query of `g_i64` has a `PREWHERE` of its own, to
-- which the restricted passes add their filter by the values of the key.
SELECT 'bounded, certified by the first restricted pass';
SELECT g, s, count() AS c FROM t_bounded GROUP BY g, s ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'bounded_01_uint32';
SELECT g, s, count() AS c FROM t_bounded GROUP BY g, s ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;
SELECT g_i64, s, count() AS c FROM t_bounded PREWHERE id % 1000 != 999 GROUP BY g_i64, s ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'bounded_02_int64';
SELECT g_i64, s, count() AS c FROM t_bounded PREWHERE id % 1000 != 999 GROUP BY g_i64, s ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;

-- These 20 values hold only small groups of `(g, t)`, so the other values may hold larger ones, and the hot
-- pass aggregates them.
SELECT 'bounded, hot pass';
SELECT g_i32, t, count() AS c FROM t_bounded GROUP BY g_i32, t ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'bounded_03_hot_int32';
SELECT g_i32, t, count() AS c FROM t_bounded GROUP BY g_i32, t ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;
SELECT g_u64, t, count() AS c FROM t_bounded GROUP BY g_u64, t ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'bounded_04_hot_uint64';
SELECT g_u64, t, count() AS c FROM t_bounded GROUP BY g_u64, t ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;

-- `EXPLAIN ANALYZE` reads the steps of the processors when the query has finished, also those of the passes.
SELECT 'explain analyze of the passes';
SELECT count() > 0 FROM (EXPLAIN ANALYZE SELECT g, s, count() AS c FROM t_bounded GROUP BY g, s ORDER BY c DESC LIMIT 5)
SETTINGS log_comment = 'bounded_05_explain_analyze';

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['AdaptiveAggregationMultiPassTopKPilotPasses'] AS pilot_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKSketchPasses'] AS sketch_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKRestrictedPasses'] AS restricted_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKDeclines'] AS declines
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'bounded_%'
ORDER BY log_comment;

DROP TABLE t_bounded;
