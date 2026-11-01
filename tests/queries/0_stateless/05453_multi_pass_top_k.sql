-- The multi-pass top-K aggregation of `GROUP BY ... ORDER BY <count or uniqExact> DESC LIMIT n` over
-- `MergeTree`: every query runs with the optimization and without it, which must return the same rows, and
-- the profile events show which passes ran: after the pilot, the candidate pass alone when it proves the top, or
-- with the fallback for the cells it could not prove, unless the pilot declines. For `uniqExact` the cells without
-- candidates count runs of repeated rows instead of rows, which certifies more.

-- The paths depend on the granules the pilot samples and on its chunks, so the granules hold 128 rows each
-- and every chunk one granule. The settings that would turn the optimization off or change the shape of the
-- plan are pinned.
SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, enable_adaptive_aggregator = 1, adaptive_aggregator_top_k = 1;
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 128;
SET optimize_aggregation_in_order = 0, max_rows_to_group_by = 0, max_rows_to_read = 0, max_bytes_to_read = 0;
SET max_bytes_to_read_leaf = 0, max_execution_speed = 0, max_execution_speed_bytes = 0, max_estimated_execution_time = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, serialize_query_plan = 0;
SET group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
-- Without the hash table statistics the declines of earlier runs are not kept, so every run takes its pilot.
SET collect_hash_table_stats_during_aggregation = 0;

DROP TABLE IF EXISTS t_top_k;

-- 500000 rows in granules of exactly 128 rows, which only Wide parts allow: the pilot samples every 32nd
-- granule.
-- `k`: the keys `a0` to `a8` hold 9, 8, ..., 1 rows of every 1000, spread over the table; every other key holds
-- one row.
-- `h`: `k`, but the rows from 256 to 4095, in granules 2 to 31, hold the key `heavy`, which ranks third and
-- which the pilot never samples.
-- `r` and `v`: `k` and `u`, but the same rows hold the key `bot`, whose values in `v` repeat in runs of 64
-- rows: it has 3840 rows, but only 60 distinct values in 60 runs.
-- `runs`: key `i` holds `2i + 1` consecutive rows, so the values come in runs.
-- `flat`: the keys `0` to `4` hold 42 rows, all but two of them in the first granule, and the other keys 2 or
-- 3 rows, so the sample shows no clear top.
-- `few`: 7 keys, so the candidates hold nearly all the rows.
CREATE TABLE t_top_k
(
    id UInt64,
    k String,
    h String,
    r String,
    runs String,
    flat String,
    few String,
    u UInt64,
    v UInt64,
    e UInt16,
    n UInt64
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_top_k
SELECT
    number AS id,
    multiIf(m < 9, 'a0', m < 17, 'a1', m < 24, 'a2', m < 30, 'a3', m < 35, 'a4', m < 39, 'a5', m < 42, 'a6', m < 44, 'a7', m < 45, 'a8',
        concat('tail', toString(number))) AS k,
    if(number >= 256 AND number < 4096, 'heavy', k) AS h,
    if(number >= 256 AND number < 4096, 'bot', k) AS r,
    toString(toUInt64(sqrt(number))) AS runs,
    toString(if(number < 200, number % 5, number % 200000)) AS flat,
    toString(toUInt64(sqrt(number % 49))) AS few,
    number AS u,
    if(number >= 256 AND number < 4096, intDiv(number, 64), number) AS v,
    toUInt16(m < 5) AS e,
    cityHash64(k) AS n
FROM (SELECT number, number % 1000 AS m FROM numbers(500000))
SETTINGS max_block_size = 1000000, min_insert_block_size_rows = 1000000;

OPTIMIZE TABLE t_top_k FINAL;

SELECT 'certified, count';
SELECT k, count() AS c FROM t_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_01_count';
SELECT k, count() AS c FROM t_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

SELECT 'certified, uniqExact without a count';
SELECT k, uniqExact(u) AS x FROM t_top_k GROUP BY k ORDER BY x DESC LIMIT 5 SETTINGS log_comment = 'top_k_02_uniq_exact';
SELECT k, uniqExact(u) AS x FROM t_top_k GROUP BY k ORDER BY x DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

SELECT 'certified, other aggregates';
SELECT k, count() AS c, sum(u), min(id), avg(e), argMax(k, id) FROM t_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_03_aggregates';
SELECT k, count() AS c, sum(u), min(id), avg(e), argMax(k, id) FROM t_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

SELECT 'certified, two keys';
SELECT e, k, count() AS c FROM t_top_k GROUP BY e, k ORDER BY c DESC LIMIT 3 SETTINGS log_comment = 'top_k_04_two_keys';
SELECT e, k, count() AS c FROM t_top_k GROUP BY e, k ORDER BY c DESC LIMIT 3 SETTINGS adaptive_aggregator_top_k = 0;

SELECT 'certified, numeric key';
SELECT n, count() AS c FROM t_top_k GROUP BY n ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_05_numeric_key';
SELECT n, count() AS c FROM t_top_k GROUP BY n ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

SELECT 'certified, array key';
SELECT [n] AS a, count() AS c FROM t_top_k GROUP BY a ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_06_array_key';
SELECT [n] AS a, count() AS c FROM t_top_k GROUP BY a ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

SELECT 'certified, filter';
SELECT k, count() AS c FROM t_top_k WHERE u % 3 != 0 GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_07_filter';
SELECT k, count() AS c FROM t_top_k WHERE u % 3 != 0 GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

SELECT 'certified, offset';
SELECT k, count() AS c FROM t_top_k GROUP BY k ORDER BY c DESC LIMIT 3 OFFSET 2 SETTINGS log_comment = 'top_k_08_offset';
SELECT k, count() AS c FROM t_top_k GROUP BY k ORDER BY c DESC LIMIT 3 OFFSET 2 SETTINGS adaptive_aggregator_top_k = 0;

SELECT 'fallback';
SELECT h, count() AS c FROM t_top_k GROUP BY h ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_09_fallback';
SELECT h, count() AS c FROM t_top_k GROUP BY h ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

-- The cell of `bot` holds more rows than the fifth best key has distinct values, but fewer runs.
SELECT 'certified by runs, uniqExact';
SELECT r, uniqExact(v) AS x FROM t_top_k GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS log_comment = 'top_k_10_runs_uniq_exact';
SELECT r, uniqExact(v) AS x FROM t_top_k GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

-- A row that the condition drops ends a run and does not count.
SELECT 'certified by runs, uniqExactIf';
SELECT r, uniqExactIf(v, id % 3 != 0) AS x FROM t_top_k GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS log_comment = 'top_k_11_runs_uniq_exact_if';
SELECT r, uniqExactIf(v, id % 3 != 0) AS x FROM t_top_k GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

-- The values of `heavy` never repeat, so its cell counts all its rows.
SELECT 'fallback, uniqExact';
SELECT h, uniqExact(u) AS x FROM t_top_k GROUP BY h ORDER BY x DESC LIMIT 5 SETTINGS log_comment = 'top_k_12_fallback_uniq_exact';
SELECT h, uniqExact(u) AS x FROM t_top_k GROUP BY h ORDER BY x DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

-- A condition that is not `UInt8` stays among the values, and the rows of a `Nullable` column are not compared, so
-- they all count.
SELECT 'fallback, uniqExactIf with a Nullable condition';
SELECT r, uniqExactIf(v, toNullable(id % 3 != 0)) AS x FROM t_top_k GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS log_comment = 'top_k_13_fallback_nullable_condition';
SELECT r, uniqExactIf(v, toNullable(id % 3 != 0)) AS x FROM t_top_k GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

SELECT 'declined, runs';
SELECT runs, count() AS c FROM t_top_k GROUP BY runs ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_14_runs';
SELECT runs, count() AS c FROM t_top_k GROUP BY runs ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

SELECT 'declined, no clear top';
SELECT * FROM (SELECT flat, count() AS c FROM t_top_k GROUP BY flat ORDER BY c DESC LIMIT 5) ORDER BY flat SETTINGS log_comment = 'top_k_15_flat';
SELECT * FROM (SELECT flat, count() AS c FROM t_top_k GROUP BY flat ORDER BY c DESC LIMIT 5) ORDER BY flat SETTINGS adaptive_aggregator_top_k = 0;

SELECT 'declined, few keys';
SELECT few, count() AS c FROM t_top_k GROUP BY few ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_16_few';
SELECT few, count() AS c FROM t_top_k GROUP BY few ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0;

-- With the hash table statistics, the next run of a query whose pilot declined skips the pilot.
SELECT 'declines of earlier runs';
SET collect_hash_table_stats_during_aggregation = 1;
SELECT few, count() AS c FROM t_top_k GROUP BY few ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'top_k_17_first';
SELECT few, count() AS c FROM t_top_k GROUP BY few ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'top_k_18_again';

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['AdaptiveAggregationMultiPassTopKPilotPasses'] AS pilot_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKCandidatePasses'] AS candidate_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKFallbackPasses'] AS fallback_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKDeclines'] AS declines
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'top_k_%'
ORDER BY log_comment;

DROP TABLE t_top_k;
