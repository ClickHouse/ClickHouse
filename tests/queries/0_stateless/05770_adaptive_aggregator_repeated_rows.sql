-- The frozen tables of the adaptive aggregator do not stage a missed row that repeats the keys and arguments of the
-- previous row when every aggregate function ignores repeated values. Each adaptive query must return the result of the
-- ordinary aggregation. The tables freeze at 8 keys, so most rows miss them.

SET max_threads = 4, max_block_size = 1024, adaptive_aggregator_freeze_threshold = 8, adaptive_aggregator_disable_thaw = 1;
SET collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;

-- 400 keys of 512 consecutive rows. xn: 12 NaN, then intDiv(p, 100). y and c are constant over 5 rows, and so is s,
-- except that row 7 differs from row 6 only by a String of the same length.
-- y16, y32 and fs differ from the previous row in one row each, in their high bytes. e is empty in the first half of
-- each key and s in the second.
DROP VIEW IF EXISTS v_repeated_rows;
CREATE VIEW v_repeated_rows AS
SELECT intDiv(number, 512) AS k, number % 512 AS p, if(p < 12, nan, toFloat64(intDiv(p, 100))) AS xn,
    intDiv(p, 5) AS y, toString(if(p = 7, 8, y % 7)) AS s, y % 2 = 0 AS c,
    toUInt16(if(p = 17, 256 + y, y)) AS y16,
    toUInt32(if(p = 22, 65536 + y, y)) AS y32,
    toFixedString(concat('fixed-string-', toString(if(p = 27, 9, y % 7))), 16) AS fs,
    if(p < 256, '', s) AS e
FROM numbers_mt(400 * 512);

-- uniqUpTo keeps a float and compares it with `==`, so each NaN counts.
SELECT 'uniqUpTo, NaN';
SET log_comment = '05770_nan';
SELECT count(), sum(u) FROM (SELECT k, uniqUpTo(20)(xn) AS u FROM v_repeated_rows GROUP BY k) SETTINGS enable_adaptive_aggregator = 1;
SET log_comment = '';
SELECT count(), sum(u) FROM (SELECT k, uniqUpTo(20)(xn) AS u FROM v_repeated_rows GROUP BY k) SETTINGS enable_adaptive_aggregator = 0;
SELECT count(), sum(u) FROM (SELECT k, uniqUpToIf(20)(toFloat32(xn), c) AS u FROM v_repeated_rows GROUP BY k) SETTINGS enable_adaptive_aggregator = 1;
SELECT count(), sum(u) FROM (SELECT k, uniqUpToIf(20)(toFloat32(xn), c) AS u FROM v_repeated_rows GROUP BY k) SETTINGS enable_adaptive_aggregator = 0;

SELECT 'ignoring repeated values';
SET log_comment = '05770_skip';
SELECT count(), sum(cityHash64(*)) FROM (SELECT k, uniqExact(xn), uniq(y), uniqUpTo(10)(y), uniqUpTo(3)(s), max(y), min(s), arraySort(groupUniqArray(s)), uniqIf(y, c), uniqUpTo(30)(xn, y), uniq(y16), uniqExact(y32), uniqExact(fs) FROM v_repeated_rows GROUP BY k) SETTINGS enable_adaptive_aggregator = 1;
SET log_comment = '';
SELECT count(), sum(cityHash64(*)) FROM (SELECT k, uniqExact(xn), uniq(y), uniqUpTo(10)(y), uniqUpTo(3)(s), max(y), min(s), arraySort(groupUniqArray(s)), uniqIf(y, c), uniqUpTo(30)(xn, y), uniq(y16), uniqExact(y32), uniqExact(fs) FROM v_repeated_rows GROUP BY k) SETTINGS enable_adaptive_aggregator = 0;

-- Without GROUP BY, uniqUpTo adds each value of a LowCardinality argument once.
SELECT uniqUpTo(3)(toLowCardinality(s)), uniqUpTo(100)(toLowCardinality(s)), uniqUpTo(3)(s), uniqUpTo(100)(s) FROM v_repeated_rows;

-- Both adaptive queries froze their tables, and the second one staged less than half as many records.
SYSTEM FLUSH LOGS query_log;
SELECT
    countIf(log_comment = '05770_nan' AND ProfileEvents['AdaptiveAggregationLocalFreezes'] > 0),
    countIf(log_comment = '05770_skip' AND ProfileEvents['AdaptiveAggregationLocalFreezes'] > 0),
    2 * sumIf(ProfileEvents['AdaptiveAggregationStagedRecords'], log_comment = '05770_skip')
        < sumIf(ProfileEvents['AdaptiveAggregationStagedRecords'], log_comment = '05770_nan')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05770_nan', '05770_skip') AND event_date >= yesterday();

-- A key of a fixed-width value and an empty String that repeats the last missed key skips its probe, and is skipped as
-- a repeat as well: the query stages less than half as many records as one with a function that does not ignore them.
SELECT 'fixed-width and String keys';
SET enable_fixed_and_packed_string_keys_in_aggregation = 1;
SET log_comment = '05770_packed_skip';
SELECT count(), sum(cityHash64(*)) FROM (SELECT k, e, uniqExact(y), max(y), uniqIf(y, c) FROM v_repeated_rows GROUP BY k, e) SETTINGS enable_adaptive_aggregator = 1;
SET log_comment = '05770_packed_no_skip';
SELECT k, e, sum(y) FROM v_repeated_rows GROUP BY k, e SETTINGS enable_adaptive_aggregator = 1 FORMAT Null;
SET log_comment = '';
SELECT count(), sum(cityHash64(*)) FROM (SELECT k, e, uniqExact(y), max(y), uniqIf(y, c) FROM v_repeated_rows GROUP BY k, e) SETTINGS enable_adaptive_aggregator = 0;
SYSTEM FLUSH LOGS query_log;
SELECT
    countIf(log_comment = '05770_packed_skip' AND ProfileEvents['AdaptiveAggregationLocalFreezes'] > 0),
    2 * sumIf(ProfileEvents['AdaptiveAggregationStagedRecords'], log_comment = '05770_packed_skip')
        < sumIf(ProfileEvents['AdaptiveAggregationStagedRecords'], log_comment = '05770_packed_no_skip')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05770_packed_skip', '05770_packed_no_skip')
    AND event_date >= yesterday();

-- The skipped rows still count in the thaw decision, so a stream of key runs thaws as it does without the skip.
SELECT 'thaw';
SET adaptive_aggregator_disable_thaw = 0;
SET log_comment = '05770_thaw';
SELECT count(), sum(u) FROM (SELECT intDiv(number, 512) AS k, uniqExact(intDiv(number % 512, 5)) AS u FROM numbers_mt(2000 * 512) GROUP BY k) SETTINGS enable_adaptive_aggregator = 1;
SET log_comment = '';
SELECT count(), sum(u) FROM (SELECT intDiv(number, 512) AS k, uniqExact(intDiv(number % 512, 5)) AS u FROM numbers_mt(2000 * 512) GROUP BY k) SETTINGS enable_adaptive_aggregator = 0;
SYSTEM FLUSH LOGS query_log;
SELECT countIf(ProfileEvents['AdaptiveAggregationLocalFreezes'] > 0), countIf(ProfileEvents['AdaptiveAggregationThaws'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05770_thaw' AND event_date >= yesterday();

-- A skipped row counts as the record it repeats, with its bytes: key runs whose first rows repeat one long String
-- thaw as they do without the skip.
SET log_comment = '05770_thaw_string';
SELECT count(), sum(u) FROM (SELECT k, uniqExact(s) AS u FROM (SELECT intDiv(number, 32) AS k, number % 32 AS p,
    if(p < 24, concat(repeat('x', 64), toString(k)), toString(number)) AS s FROM numbers_mt(500000)) GROUP BY k)
    SETTINGS enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1024, max_block_size = 8192;
SET log_comment = '';
SELECT count(), sum(u) FROM (SELECT k, uniqExact(s) AS u FROM (SELECT intDiv(number, 32) AS k, number % 32 AS p,
    if(p < 24, concat(repeat('x', 64), toString(k)), toString(number)) AS s FROM numbers_mt(500000)) GROUP BY k)
    SETTINGS enable_adaptive_aggregator = 0;
SYSTEM FLUSH LOGS query_log;
SELECT countIf(ProfileEvents['AdaptiveAggregationLocalFreezes'] > 0), countIf(ProfileEvents['AdaptiveAggregationThaws'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05770_thaw_string' AND event_date >= yesterday();

DROP VIEW v_repeated_rows;
