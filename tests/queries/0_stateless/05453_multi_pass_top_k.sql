-- The multi-pass top-K aggregation of `GROUP BY ... ORDER BY <count or uniqExact> DESC LIMIT n` over
-- `MergeTree`. Every query runs with the optimization and without it, which must return the same rows, and
-- the profile events show which passes ran. In the candidate mode, the pilot is followed by the candidate
-- pass, and by the fallback for the cells that the candidate pass could not prove, unless the pilot declines.
-- For `uniqExact`, the cells without candidates count runs of repeated rows instead of rows, which certifies
-- more. When the sample cannot show the top and an integer key bounds the groups, the bounding mode runs the
-- sketch pass, the first restricted pass over the values of the key with most rows, and the hot pass over the
-- other values that may hold a group of the top.

-- The paths depend on the granules that the pilot samples and on its chunks, so the granules hold 64 rows
-- each and every chunk holds one granule. The settings that would turn the optimization off or change the
-- shape of the plan are pinned.
SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, enable_adaptive_aggregator = 1, adaptive_aggregator_top_k = 1;
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 64;
SET optimize_aggregation_in_order = 0, max_rows_to_group_by = 0, max_rows_to_read = 0, max_bytes_to_read = 0;
SET max_bytes_to_read_leaf = 0, max_execution_speed = 0, max_execution_speed_bytes = 0, max_estimated_execution_time = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, serialize_query_plan = 0;
SET group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
-- Without the hash table statistics, the declines of earlier runs are not kept, so every run takes its pilot.
SET collect_hash_table_stats_during_aggregation = 0;

DROP TABLE IF EXISTS t_top_k;

-- 67584 rows in a single Wide part of 1056 granules of exactly 64 rows. The optimization needs at least 1024
-- granules, and the pilot samples every 32nd one. The insert writes one part, so no merge changes the
-- granules.
-- `k`: the keys `a0` to `a8` hold 12, 11, 10, 8, 6, 4, 3, 2 and 1 of every 199 rows, spread over the table.
-- Every other key holds one row.
-- `h`: `k`, but the rows of granules 1 to 40, except 32, hold the key `heavy`. It ranks fifth, and the pilot
-- never samples it.
-- `r` and `v`: `k` and `id`, but the same rows hold the key `bot`, whose values in `v` repeat in runs of 64
-- rows. It has 2496 rows, but only 39 distinct values in 39 runs.
-- `g`, and its copies of the other integer types that a bounding key may have: the value `i` holds `2i + 1`
-- consecutive rows, so the values come in runs. The signed copies hold negative values, and the 64-bit copies
-- hold values beyond 32 bits.
-- `s`: a single value, so the groups of `(g, s)` are the runs of `g`.
-- `t`: splits each run of `g` from the value 200 on into 5 groups, so the values of `g` with most rows hold
-- only small groups.
CREATE TABLE t_top_k
(
    id UInt64,
    g UInt32,
    g_i32 Int32,
    g_i64 Int64,
    g_u64 UInt64,
    k String,
    h String,
    r String,
    v UInt64,
    s String,
    t String
)
ENGINE = MergeTree ORDER BY (g, id)
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_top_k
SELECT
    number AS id,
    toUInt32(sqrt(number)) AS g,
    -toInt32(g) AS g_i32,
    -toInt64(g) - 5000000000 AS g_i64,
    toUInt64(g) + 0xF000000000000000 AS g_u64,
    multiIf(m < 12, 'a0', m < 23, 'a1', m < 33, 'a2', m < 41, 'a3', m < 47, 'a4', m < 51, 'a5', m < 54, 'a6', m < 56, 'a7',
        m < 57, 'a8', concat('tail', toString(number))) AS k,
    if(in_block, 'heavy', k) AS h,
    if(in_block, 'bot', k) AS r,
    if(in_block, intDiv(number, 64), number) AS v,
    'x' AS s,
    if(g >= 200, toString(number % 5), 'x') AS t
FROM (SELECT number, number % 199 AS m, number BETWEEN 64 AND 2623 AND intDiv(number, 64) != 32 AS in_block FROM numbers(67584))
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

SELECT 'certified, count and other aggregates';
SELECT k, count() AS c, sum(id), min(id), avg(id % 7), argMax(g, id) FROM t_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_01_count';
SELECT k, count() AS c, sum(id), min(id), avg(id % 7), argMax(g, id) FROM t_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

SELECT 'certified, two keys and an offset';
SELECT toUInt16(id % 199 < 5) AS e, k, count() AS c FROM t_top_k GROUP BY e, k ORDER BY c DESC LIMIT 3 OFFSET 2 SETTINGS log_comment = 'top_k_02_two_keys_offset';
SELECT toUInt16(id % 199 < 5) AS e, k, count() AS c FROM t_top_k GROUP BY e, k ORDER BY c DESC LIMIT 3 OFFSET 2 SETTINGS adaptive_aggregator_top_k = 0;

SELECT 'certified, numeric key and a filter';
SELECT cityHash64(k) AS n, count() AS c FROM t_top_k WHERE id % 3 != 0 GROUP BY n ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_03_numeric_key_filter';
SELECT cityHash64(k) AS n, count() AS c FROM t_top_k WHERE id % 3 != 0 GROUP BY n ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

SELECT 'certified, array key';
SELECT [cityHash64(k)] AS a, count() AS c FROM t_top_k GROUP BY a ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_04_array_key';
SELECT [cityHash64(k)] AS a, count() AS c FROM t_top_k GROUP BY a ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

SELECT 'fallback';
SELECT h, count() AS c FROM t_top_k GROUP BY h ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_05_fallback';
SELECT h, count() AS c FROM t_top_k GROUP BY h ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

-- The cell of `bot` holds more rows than the fifth best key has distinct values, but fewer runs.
SELECT 'certified by runs, uniqExact';
SELECT r, uniqExact(v) AS x FROM t_top_k GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS log_comment = 'top_k_06_runs_uniq_exact';
SELECT r, uniqExact(v) AS x FROM t_top_k GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

-- A row that the condition drops ends a run and does not count.
SELECT 'certified by runs, uniqExactIf';
SELECT r, uniqExactIf(v, id % 3 != 0) AS x FROM t_top_k GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS log_comment = 'top_k_07_runs_uniq_exact_if';
SELECT r, uniqExactIf(v, id % 3 != 0) AS x FROM t_top_k GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

-- A condition that is not `UInt8` stays among the values. The rows of a `Nullable` column are not compared,
-- so they all count.
SELECT 'fallback, uniqExactIf with a Nullable condition';
SELECT r, uniqExactIf(v, toNullable(id % 3 != 0)) AS x FROM t_top_k GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS log_comment = 'top_k_08_fallback_nullable_condition';
SELECT r, uniqExactIf(v, toNullable(id % 3 != 0)) AS x FROM t_top_k GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

-- A query of a single key has no bounding key, so the pilot declines.
SELECT 'declined, runs';
SELECT toString(g) AS runs, count() AS c FROM t_top_k GROUP BY runs ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_09_runs';
SELECT toString(g) AS runs, count() AS c FROM t_top_k GROUP BY runs ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

-- The keys `0` to `4` hold 43 rows each, 40 of them in the first 200 rows, and the other keys at most 4. The
-- sample holds 12 or 13 rows of each of them, so it shows no clear top.
SELECT 'declined, no clear top';
SELECT * FROM (SELECT toString(if(id < 200, id % 5, id % 20000)) AS flat, count() AS c FROM t_top_k GROUP BY flat ORDER BY c DESC LIMIT 5) ORDER BY flat SETTINGS log_comment = 'top_k_10_flat';
SELECT * FROM (SELECT toString(if(id < 200, id % 5, id % 20000)) AS flat, count() AS c FROM t_top_k GROUP BY flat ORDER BY c DESC LIMIT 5) ORDER BY flat SETTINGS adaptive_aggregator_top_k = 0;

-- 7 keys, so the candidates hold nearly all the rows.
SELECT 'declined, few keys';
SELECT toString(toUInt64(sqrt(id % 49))) AS few, count() AS c FROM t_top_k GROUP BY few ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_11_few';
SELECT toString(toUInt64(sqrt(id % 49))) AS few, count() AS c FROM t_top_k GROUP BY few ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

-- The bounding mode by a key of each integer type. Its first restricted pass takes the 20 values of the key
-- with most rows, 4 for every group of the top. `g` is part of the primary key, so the restricted passes read
-- only the granules of its values. Its copies are not part of it.
-- These 20 values hold the 5 best groups of `(g, s)`, and no other value holds as many rows as the fifth best
-- group, so the first restricted pass proves the top.
SELECT 'bounded, certified by the first restricted pass';
SELECT g, s, count() AS c FROM t_top_k GROUP BY g, s ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_12_bounded_uint32';
SELECT g, s, count() AS c FROM t_top_k GROUP BY g, s ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;
SELECT g_i64, s, count() AS c FROM t_top_k GROUP BY g_i64, s ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_13_bounded_int64';
SELECT g_i64, s, count() AS c FROM t_top_k GROUP BY g_i64, s ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

-- These 20 values hold only small groups of `(g, t)`, so the other values may hold larger ones, and the hot
-- pass aggregates them.
SELECT 'bounded, hot pass';
SELECT g_i32, t, count() AS c FROM t_top_k GROUP BY g_i32, t ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_14_hot_int32';
SELECT g_i32, t, count() AS c FROM t_top_k GROUP BY g_i32, t ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;
SELECT g_u64, t, count() AS c FROM t_top_k GROUP BY g_u64, t ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_15_hot_uint64';
SELECT g_u64, t, count() AS c FROM t_top_k GROUP BY g_u64, t ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

-- With the hash table statistics, the next run of a query whose pilot declined skips the pilot.
SELECT 'declines of earlier runs';
SET collect_hash_table_stats_during_aggregation = 1;
SELECT toString(toUInt64(sqrt(id % 49))) AS few, count() AS c FROM t_top_k GROUP BY few ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'top_k_16_first';
SELECT toString(toUInt64(sqrt(id % 49))) AS few, count() AS c FROM t_top_k GROUP BY few ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'top_k_17_again';

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['AdaptiveAggregationMultiPassTopKPilotPasses'] AS pilot_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKCandidatePasses'] AS candidate_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKFallbackPasses'] AS fallback_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKSketchPasses'] AS sketch_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKRestrictedPasses'] AS restricted_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKDeclines'] AS declines
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'top_k_%'
ORDER BY log_comment;

DROP TABLE t_top_k;
