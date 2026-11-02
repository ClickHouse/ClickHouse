-- The multi-pass top-K aggregation holds the output of a pass until the pass finishes, so every pass outputs
-- no more groups than its mode reads. The profile of the processors shows the rows that the controller of the
-- passes receives. A `Nullable` key keeps the aggregation of a pass from selecting the best groups of its
-- buckets itself. The first restricted pass and the hot pass of the bounding mode output their best groups,
-- as many as the top takes. The fallback outputs as many, or the groups that the run learns from. When the
-- values of the bounding key that the first restricted pass takes hold most rows, the bounding mode declines
-- after the sketch, as that pass would repeat most of the original aggregation.

SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, enable_adaptive_aggregator = 1, adaptive_aggregator_top_k = 1;
SET adaptive_aggregator_top_k_cache_candidates = 1;
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 64, optimize_aggregation_in_order = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, serialize_query_plan = 0;
SET group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
-- The read cannot estimate its bytes with the injection that splits its ranges, so a limit of the bytes to
-- read would keep the optimization off.
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET collect_hash_table_stats_during_aggregation = 0, log_processors_profiles = 1;

-- Every table holds 67584 rows in a single part of 1056 granules of 64 rows, and the pilot samples every 32nd
-- granule. A query that runs no pass reads blocks of 65536 rows, which leave its result unchanged: the
-- executor takes locks for every block, and the thread fuzzer of the flaky check slows down every lock.
DROP TABLE IF EXISTS t_bound;
DROP TABLE IF EXISTS t_fallback;
DROP TABLE IF EXISTS t_constant;

-- Every value of `g` holds 512 consecutive rows: a group of `40 + g % 100` rows, and then groups of one row.
-- The sample shows the large groups in single granules, so the bounding mode runs. Its first restricted pass
-- takes 20 values of `g`, with about 8000 groups.
CREATE TABLE t_bound (g UInt32, s Nullable(String)) ENGINE = MergeTree ORDER BY g
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_bound SELECT intDiv(number, 512) AS g, if(number % 512 < 40 + g % 100, concat('heavy', toString(g)), toString(number)) FROM numbers(67584)
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

-- The keys `s0` to `s4` hold one row of every granule, and become the candidates. The keys `h0` to `h29` hold
-- 1100 to 1680 rows within the gaps between the sampled granules, so the candidate pass cannot prove the top,
-- and the fallback aggregates their cells. Every other key holds one row.
CREATE TABLE t_fallback (k String, x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_fallback
SELECT
    multiIf(offset < 5, concat('s', toString(offset)),
        gap < 30 AND granule % 32 != 0 AND hidden_rank < 1100 + 20 * gap, concat('h', toString(gap)),
        concat('t', toString(number))),
    number
FROM
(
    SELECT number, number % 64 AS offset, intDiv(number, 64) AS granule, intDiv(granule, 32) AS gap,
        (granule % 32 - 1) * 59 + (offset - 5) AS hidden_rank
    FROM numbers(67584)
)
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

-- A single value of the bounding key, which holds all the rows.
CREATE TABLE t_constant (k Int64, s Nullable(String)) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_constant SELECT 1, if(number < 64, 'heavy', toString(number)) FROM numbers(67584)
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

SELECT 'restricted and hot passes';
SELECT g, s, count() AS c FROM t_bound GROUP BY g, s ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'outputs_01_restricted_and_hot';
SELECT g, s, count() AS c FROM t_bound GROUP BY g, s ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;

-- The run that learns keeps the groups of the fallback for the next run, which then proves the top at once.
SELECT 'fallback';
SELECT toNullable(k) AS nk, count() AS c FROM t_fallback GROUP BY nk ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'outputs_02_fallback';
SELECT toNullable(k) AS nk, count() AS c FROM t_fallback GROUP BY nk ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'outputs_03_fallback_learns', collect_hash_table_stats_during_aggregation = 1;
SELECT toNullable(k) AS nk, count() AS c FROM t_fallback GROUP BY nk ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'outputs_04_learned', collect_hash_table_stats_during_aggregation = 1;
SELECT toNullable(k) AS nk, count() AS c FROM t_fallback GROUP BY nk ORDER BY c DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;

SELECT 'heaviest values hold most rows';
SELECT k, s, count() AS c FROM t_constant GROUP BY k, s ORDER BY c DESC LIMIT 1 SETTINGS log_comment = 'outputs_05_single_value';

SYSTEM FLUSH LOGS query_log, processors_profile_log;
SELECT
    log_comment,
    pilot_passes, candidate_passes, fallback_passes, sketch_passes, restricted_passes, declines,
    controller_rows
FROM
(
    SELECT
        log_comment,
        query_id,
        ProfileEvents['AdaptiveAggregationMultiPassTopKPilotPasses'] AS pilot_passes,
        ProfileEvents['AdaptiveAggregationMultiPassTopKCandidatePasses'] AS candidate_passes,
        ProfileEvents['AdaptiveAggregationMultiPassTopKFallbackPasses'] AS fallback_passes,
        ProfileEvents['AdaptiveAggregationMultiPassTopKSketchPasses'] AS sketch_passes,
        ProfileEvents['AdaptiveAggregationMultiPassTopKRestrictedPasses'] AS restricted_passes,
        ProfileEvents['AdaptiveAggregationMultiPassTopKDeclines'] AS declines
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'outputs_%'
) AS queries
LEFT JOIN
(
    SELECT query_id, sum(input_rows) AS controller_rows
    FROM system.processors_profile_log
    WHERE name = 'MultiPassTopKController'
        AND query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND log_comment LIKE 'outputs_%')
    GROUP BY query_id
) AS controllers USING (query_id)
ORDER BY log_comment;

DROP TABLE t_bound;
DROP TABLE t_fallback;
DROP TABLE t_constant;
