-- The multi-pass top-K aggregation of `GROUP BY ... ORDER BY count() DESC LIMIT n` over `MergeTree` in the
-- candidate mode. Every query runs with the optimization and without it, which must return the same rows, and
-- the profile events show which passes ran. The pilot is followed by the candidate pass, and by the fallback
-- for the cells that the candidate pass could not prove, unless the pilot declines.

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
-- `g`: the value `i` holds `2i + 1` consecutive rows, so the values come in runs.
CREATE TABLE t_top_k
(
    id UInt64,
    g UInt32,
    k String,
    h String
)
ENGINE = MergeTree ORDER BY (g, id)
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_top_k
SELECT
    number AS id,
    toUInt32(sqrt(number)) AS g,
    multiIf(m < 12, 'a0', m < 23, 'a1', m < 33, 'a2', m < 41, 'a3', m < 47, 'a4', m < 51, 'a5', m < 54, 'a6', m < 56, 'a7',
        m < 57, 'a8', concat('tail', toString(number))) AS k,
    if(in_block, 'heavy', k) AS h
FROM (SELECT number, number % 199 AS m, number BETWEEN 64 AND 2623 AND intDiv(number, 64) != 32 AS in_block FROM numbers(67584))
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

SELECT 'certified, count and other aggregates';
SELECT k, count() AS c, sum(id), min(id), avg(id % 7), argMax(g, id) FROM t_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_01_count';
SELECT k, count() AS c, sum(id), min(id), avg(id % 7), argMax(g, id) FROM t_top_k GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;

SELECT 'certified, two keys and an offset';
SELECT toUInt16(id % 199 < 5) AS e, k, count() AS c FROM t_top_k GROUP BY e, k ORDER BY c DESC LIMIT 3 OFFSET 2 SETTINGS log_comment = 'top_k_02_two_keys_offset';
SELECT toUInt16(id % 199 < 5) AS e, k, count() AS c FROM t_top_k GROUP BY e, k ORDER BY c DESC LIMIT 3 OFFSET 2 SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;

SELECT 'certified, numeric key and a filter';
SELECT cityHash64(k) AS n, count() AS c FROM t_top_k WHERE id % 3 != 0 GROUP BY n ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_03_numeric_key_filter';
SELECT cityHash64(k) AS n, count() AS c FROM t_top_k WHERE id % 3 != 0 GROUP BY n ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;

SELECT 'certified, array key';
SELECT [cityHash64(k)] AS a, count() AS c FROM t_top_k GROUP BY a ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_04_array_key';
SELECT [cityHash64(k)] AS a, count() AS c FROM t_top_k GROUP BY a ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;

SELECT 'fallback';
SELECT h, count() AS c FROM t_top_k GROUP BY h ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_05_fallback';
SELECT h, count() AS c FROM t_top_k GROUP BY h ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;

-- A query of a single key has no bounding key, so the pilot declines.
SELECT 'declined, runs';
SELECT toString(g) AS runs, count() AS c FROM t_top_k GROUP BY runs ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_06_runs';
SELECT toString(g) AS runs, count() AS c FROM t_top_k GROUP BY runs ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;

-- The keys `0` to `4` hold 43 rows each, 40 of them in the first 200 rows, and the other keys at most 4. The
-- sample holds 12 or 13 rows of each of them, so it shows no clear top.
SELECT 'declined, no clear top';
SELECT * FROM (SELECT toString(if(id < 200, id % 5, id % 20000)) AS flat, count() AS c FROM t_top_k GROUP BY flat ORDER BY c DESC LIMIT 5) ORDER BY flat SETTINGS log_comment = 'top_k_07_flat';
SELECT * FROM (SELECT toString(if(id < 200, id % 5, id % 20000)) AS flat, count() AS c FROM t_top_k GROUP BY flat ORDER BY c DESC LIMIT 5) ORDER BY flat SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;

-- 7 keys, so the candidates hold nearly all the rows.
SELECT 'declined, few keys';
SELECT toString(toUInt64(sqrt(id % 49))) AS few, count() AS c FROM t_top_k GROUP BY few ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'top_k_08_few';
SELECT toString(toUInt64(sqrt(id % 49))) AS few, count() AS c FROM t_top_k GROUP BY few ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;

-- With the hash table statistics, the next run of a query whose pilot declined skips the pilot, and so runs
-- no pass.
SELECT 'declines of earlier runs';
SET collect_hash_table_stats_during_aggregation = 1;
SELECT toString(toUInt64(sqrt(id % 49))) AS few, count() AS c FROM t_top_k GROUP BY few ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'top_k_09_first';
SELECT toString(toUInt64(sqrt(id % 49))) AS few, count() AS c FROM t_top_k GROUP BY few ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'top_k_10_again', max_block_size = 65536;

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
