-- Tags: no-parallel-replicas
-- EXPLAIN checks plans whose reading step is replaced by a prepared source.

-- Parts proved to match the filter are aggregated from statistics; other parts are read.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0;
SET aggregate_functions_null_for_empty = 0;
SET materialize_statistics_on_insert = 1, mutations_sync = 2;
SET use_statistics_for_part_pruning = 1, use_statistics_for_min_max_aggregation = 1, use_statistics_for_sum_avg_aggregation = 1;
SET optimize_functions_to_subcolumns = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_filt_stats;

CREATE TABLE t_filt_stats
(
    id UInt64,
    ts UInt64,
    cat UInt32,
    val Int64,
    dec Decimal64(2),
    n Nullable(Int64),
    f Float64,
    s String
)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic', min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_filt_stats;

-- Part 1: ts [100, 199], cat = 1, n has NULLs, f is finite
INSERT INTO t_filt_stats SELECT
    100 + number, 100 + number, 1, number * 10 - 500, toDecimal64(number, 2) / 4,
    if(number % 3 = 0, NULL, number * 2), toFloat64(number + 1), if(number = 50, 'boom', 'ok')
FROM numbers(100);

-- Part 2: ts [200, 299], cat = 2, n has no NULLs, f is NaN at number = 0
INSERT INTO t_filt_stats SELECT
    200 + number, 200 + number, 2, number * 5 + 100, toDecimal64(number + 100, 2) / 2,
    10 + number, if(number = 0, nan, toFloat64(10 + number)), if(number = 5, 'boom', 'ok')
FROM numbers(100);

-- Part 3: ts [300, 399], cat = 2, n is all NULL
INSERT INTO t_filt_stats SELECT
    300 + number, 300 + number, 2, number * 3, toDecimal64(number, 2),
    NULL, toFloat64(200 + number), 'ok'
FROM numbers(100);

-- Part 4: ts [400, 499], cat = 3, n has NULLs
INSERT INTO t_filt_stats SELECT
    400 + number, 400 + number, 3, number - 50, toDecimal64(number, 2) / 5,
    if(number % 2 = 0, NULL, number + 50), toFloat64(300 + number), 'ok'
FROM numbers(100);

SELECT '1. all matching parts answered from statistics (no ReadFromMergeTree)';
SELECT count(), count(val), count(n), min(val), max(val), min(n), max(n), sum(val), avg(val), sumCount(val), sum(dec), avg(dec), sum(n), avg(n)
FROM t_filt_stats WHERE ts >= 200 AND ts <= 399;
SELECT count(), count(val), count(n), min(val), max(val), min(n), max(n), sum(val), avg(val), sumCount(val), sum(dec), avg(dec), sum(n), avg(n)
FROM t_filt_stats WHERE ts >= 200 AND ts <= 399
SETTINGS use_statistics_for_min_max_aggregation = 0, use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT count(), count(val), count(n), min(val), max(val), min(n), max(n), sum(val), avg(val), sumCount(val), sum(dec), avg(dec), sum(n), avg(n)
      FROM t_filt_stats WHERE ts >= 200 AND ts <= 399);

-- Part 5: ts [500, 599], cat = 4, statistics not materialized
INSERT INTO t_filt_stats SETTINGS materialize_statistics_on_insert = 0 SELECT
    500 + number, 500 + number, 4, number * 7, toDecimal64(number, 2),
    if(number % 4 = 0, NULL, number), toFloat64(400 + number), 'ok'
FROM numbers(100);

SELECT '2. hybrid: pruned + exact-matched + boundary + missing-statistics parts';
SELECT count(), count(n), min(val), max(val), sum(val), avg(val), sum(n), avg(n)
FROM t_filt_stats WHERE ts >= 150 AND ts <= 550;
SELECT count(), count(n), min(val), max(val), sum(val), avg(val), sum(n), avg(n)
FROM t_filt_stats WHERE ts >= 150 AND ts <= 550
SETTINGS use_statistics_for_min_max_aggregation = 0, use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT count(), count(n), min(val), max(val), sum(val), avg(val), sum(n), avg(n)
      FROM t_filt_stats WHERE ts >= 150 AND ts <= 550);

SELECT '3. count() with non-PK filter falls back from exact_count to statistics; PK filter keeps exact_count';
SELECT count() FROM t_filt_stats WHERE ts >= 150 AND ts <= 450;
SELECT count() FROM t_filt_stats WHERE ts >= 150 AND ts <= 450
SETTINGS use_statistics_for_min_max_aggregation = 0, use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%_exact_count_projection%')
FROM (EXPLAIN SELECT count() FROM t_filt_stats WHERE ts >= 150 AND ts <= 450);
SELECT count() FROM t_filt_stats WHERE id >= 150 AND id <= 450;
SELECT countIf(explain LIKE '%_exact_count_projection%') > 0
FROM (EXPLAIN SELECT count() FROM t_filt_stats WHERE id >= 150 AND id <= 450);

SELECT '4. PREWHERE and FilterStep (optimize_move_to_prewhere = 0 and 1)';
SELECT count(), sum(val), min(val), max(val) FROM t_filt_stats PREWHERE ts >= 200 WHERE cat = 2;
SELECT count(), sum(val), min(val), max(val) FROM t_filt_stats PREWHERE ts >= 200 WHERE cat = 2
SETTINGS use_statistics_for_min_max_aggregation = 0, use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT count(), sum(val), min(val), max(val) FROM t_filt_stats PREWHERE ts >= 200 WHERE cat = 2);
SELECT count(), sum(val) FROM t_filt_stats WHERE ts >= 200 AND ts <= 399 SETTINGS optimize_move_to_prewhere = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT count(), sum(val) FROM t_filt_stats WHERE ts >= 200 AND ts <= 399 SETTINGS optimize_move_to_prewhere = 0);

SELECT '5. Nullable filter column three-valued logic';
SELECT count(), sum(val), sum(n) FROM t_filt_stats WHERE n > 0;
SELECT count(), sum(val), sum(n) FROM t_filt_stats WHERE n > 0
SETTINGS use_statistics_for_min_max_aggregation = 0, use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT count(), sum(val), sum(n) FROM t_filt_stats WHERE n > 0);
SELECT count(), sum(val) FROM t_filt_stats WHERE n != 999;
SELECT count(), sum(val) FROM t_filt_stats WHERE n != 999
SETTINGS use_statistics_for_min_max_aggregation = 0, use_statistics_for_sum_avg_aggregation = 0;
SELECT count(), sum(val) FROM t_filt_stats WHERE n IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count(), sum(val) FROM t_filt_stats WHERE n IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 0, use_statistics_for_min_max_aggregation = 0, use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT count(), sum(val) FROM t_filt_stats WHERE n IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 0);
SELECT count(), sum(val) FROM t_filt_stats WHERE n IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count(), sum(val) FROM t_filt_stats WHERE n IS NULL SETTINGS optimize_functions_to_subcolumns = 0, use_statistics_for_min_max_aggregation = 0, use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT count(), sum(val) FROM t_filt_stats WHERE n IS NULL SETTINGS optimize_functions_to_subcolumns = 0);
SELECT count(), sum(val) FROM t_filt_stats WHERE n IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count(), sum(val) FROM t_filt_stats WHERE n IS NULL SETTINGS optimize_functions_to_subcolumns = 1;

SELECT '6. Float64 NaN must never be falsely whole-matched';
SELECT count(), sum(val) FROM t_filt_stats WHERE f > 0;
SELECT count(), sum(val) FROM t_filt_stats WHERE f > 0
SETTINGS use_statistics_for_min_max_aggregation = 0, use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%')
FROM (EXPLAIN SELECT count(), sum(val) FROM t_filt_stats WHERE f > 0);
SELECT count(), sum(val) FROM t_filt_stats WHERE f != 100;
SELECT count(), sum(val) FROM t_filt_stats WHERE f != 100
SETTINGS use_statistics_for_min_max_aggregation = 0, use_statistics_for_sum_avg_aggregation = 0;

SELECT '7. Disjunction (OR) with stats columns and OR with throwing unknown branch';
SELECT count(), sum(val) FROM t_filt_stats WHERE (ts >= 200 AND ts <= 299) OR cat = 3;
SELECT count(), sum(val) FROM t_filt_stats WHERE (ts >= 200 AND ts <= 299) OR cat = 3
SETTINGS use_statistics_for_min_max_aggregation = 0, use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT count(), sum(val) FROM t_filt_stats WHERE (ts >= 200 AND ts <= 299) OR cat = 3);
SELECT countIf(explain LIKE '%_statistics_projection%')
FROM (EXPLAIN SELECT count() FROM t_filt_stats WHERE (cat = 2 AND throwIf(s = 'boom')) OR ts >= 200);
SELECT count() FROM t_filt_stats WHERE (cat = 2 AND throwIf(s = 'boom')) OR ts >= 200
SETTINGS short_circuit_function_evaluation = 'disable'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT '8. Non-deterministic filter, disabled setting, empty match';
SELECT countIf(explain LIKE '%_statistics_projection%')
FROM (EXPLAIN SELECT count(), sum(val) FROM t_filt_stats WHERE ts >= 200 AND rand() >= 0);
SELECT countIf(explain LIKE '%_statistics_projection%')
FROM (EXPLAIN SELECT count(), sum(val) FROM t_filt_stats WHERE ts >= 200 AND ts <= 399 SETTINGS use_statistics_for_part_pruning = 0);
SELECT count(), count(n), sum(val), avg(val), min(val), max(val) FROM t_filt_stats WHERE ts > 10000;
SELECT count(), count(n), sum(val), avg(val), min(val), max(val) FROM t_filt_stats WHERE ts > 10000
SETTINGS use_statistics_for_min_max_aggregation = 0, use_statistics_for_sum_avg_aggregation = 0;

SELECT '9. Stale filter column type after killed MODIFY COLUMN';
DROP TABLE IF EXISTS t_filt_stale;
CREATE TABLE t_filt_stale (x UInt64 STATISTICS(basic), y UInt64 STATISTICS(basic)) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES t_filt_stale;
INSERT INTO t_filt_stale VALUES (250, 10), (260, 20);
ALTER TABLE t_filt_stale MODIFY COLUMN x UInt8 SETTINGS mutations_sync = 0, alter_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_filt_stale' SYNC FORMAT Null;
SELECT count(), sum(y) FROM t_filt_stale WHERE x > 100;
SELECT count(), sum(y) FROM t_filt_stale WHERE x > 100
SETTINGS use_statistics_for_min_max_aggregation = 0, use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%')
FROM (EXPLAIN SELECT count(), sum(y) FROM t_filt_stale WHERE x > 100);
DROP TABLE t_filt_stale;

SELECT '10. Row policy and lightweight delete disable statistics aggregation';
DROP ROW POLICY IF EXISTS p_filt_stats ON t_filt_stats;
CREATE ROW POLICY p_filt_stats ON t_filt_stats FOR SELECT USING cat = 2 TO CURRENT_USER;
SELECT count(), sum(val) FROM t_filt_stats WHERE ts >= 200 AND ts <= 499;
SELECT count(), sum(val) FROM t_filt_stats WHERE ts >= 200 AND ts <= 499
SETTINGS use_statistics_for_min_max_aggregation = 0, use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%')
FROM (EXPLAIN SELECT count(), sum(val) FROM t_filt_stats WHERE ts >= 200 AND ts <= 499);
DROP ROW POLICY p_filt_stats ON t_filt_stats;

DROP TABLE IF EXISTS t_filt_lwd;
CREATE TABLE t_filt_lwd (id UInt64, ts UInt64, val Int64)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic';
INSERT INTO t_filt_lwd SELECT 200 + number, 200 + number, number * 5 + 100 FROM numbers(100);
DELETE FROM t_filt_lwd WHERE id = 205;
SELECT count(), sum(val) FROM t_filt_lwd WHERE ts >= 200 AND ts <= 299;
SELECT count(), sum(val) FROM t_filt_lwd WHERE ts >= 200 AND ts <= 299
SETTINGS use_statistics_for_min_max_aggregation = 0, use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%')
FROM (EXPLAIN SELECT count(), sum(val) FROM t_filt_lwd WHERE ts >= 200 AND ts <= 299);
DROP TABLE t_filt_lwd;

SELECT '11. Small cold reads do not load statistics; warm reads still use them';
DROP TABLE IF EXISTS t_filt_stats_cold;
CREATE TABLE t_filt_stats_cold (id UInt64, ts UInt64, val Int64)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic', min_bytes_for_wide_part = 0,
    max_bytes_to_merge_at_max_space_in_pool = 1, max_bytes_to_merge_at_min_space_in_pool = 1;
INSERT INTO t_filt_stats_cold SELECT number, 100 + number, number * 10 FROM numbers(100);
INSERT INTO t_filt_stats_cold SELECT 100 + number, 200 + number, number * 10 FROM numbers(100);

DETACH TABLE t_filt_stats_cold;
ATTACH TABLE t_filt_stats_cold;
SELECT countIf(explain LIKE '%_statistics_projection%')
FROM (EXPLAIN SELECT count(), sum(val) FROM t_filt_stats_cold WHERE ts >= 200 AND ts <= 299);
SELECT count(), sum(val) FROM t_filt_stats_cold WHERE ts >= 200 AND ts <= 299
SETTINGS min_marks_per_part_to_load_statistics = 0;
SELECT countIf(explain LIKE '%_statistics_projection%')
FROM (EXPLAIN SELECT count(), sum(val) FROM t_filt_stats_cold WHERE ts >= 200 AND ts <= 299);

DETACH TABLE t_filt_stats_cold;
ATTACH TABLE t_filt_stats_cold;
SELECT countIf(explain LIKE '%_statistics_projection%')
FROM (EXPLAIN SELECT count(), sum(val) FROM t_filt_stats_cold WHERE ts >= 200 AND ts <= 299
      SETTINGS min_marks_per_part_to_load_statistics = 0);
DROP TABLE t_filt_stats_cold;

DROP TABLE t_filt_stats;
