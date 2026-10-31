-- Blocks whose only GROUP BY key holds one value or a few long runs of values are aggregated like a constant key.
-- Every cell compares the result with `optimize_group_by_constant_keys = 0` by `EXCEPT ALL` in both directions and prints 0.

SET max_threads = 1;
SET max_threads_min_free_memory_per_thread = 0;

SELECT 'one value per block';
SELECT count() FROM
(
    (SELECT * FROM (SELECT intDiv(number, 30000) AS k, count(), sum(number), min(number), max(number), avg(number), uniqExact(number % 1000), sumIf(number, number % 3 = 0), countIf(number % 3 = 0), groupBitmap(toUInt32(number)), sumArray([number, 1]), finalizeAggregation(sumState(number)), sum(nullIf(number % 7, 0)), count(nullIf(number % 7, 0)) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 10000, optimize_group_by_constant_keys = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT intDiv(number, 30000) AS k, count(), sum(number), min(number), max(number), avg(number), uniqExact(number % 1000), sumIf(number, number % 3 = 0), countIf(number % 3 = 0), groupBitmap(toUInt32(number)), sumArray([number, 1]), finalizeAggregation(sumState(number)), sum(nullIf(number % 7, 0)), count(nullIf(number % 7, 0)) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 10000, optimize_group_by_constant_keys = 0))
    UNION ALL
    (SELECT * FROM (SELECT intDiv(number, 30000) AS k, count(), sum(number), min(number), max(number), avg(number), uniqExact(number % 1000), sumIf(number, number % 3 = 0), countIf(number % 3 = 0), groupBitmap(toUInt32(number)), sumArray([number, 1]), finalizeAggregation(sumState(number)), sum(nullIf(number % 7, 0)), count(nullIf(number % 7, 0)) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 10000, optimize_group_by_constant_keys = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT intDiv(number, 30000) AS k, count(), sum(number), min(number), max(number), avg(number), uniqExact(number % 1000), sumIf(number, number % 3 = 0), countIf(number % 3 = 0), groupBitmap(toUInt32(number)), sumArray([number, 1]), finalizeAggregation(sumState(number)), sum(nullIf(number % 7, 0)), count(nullIf(number % 7, 0)) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 10000, optimize_group_by_constant_keys = 1))
);

SELECT 'several runs per block, keys repeating out of order';
SELECT count() FROM
(
    (SELECT * FROM (SELECT bitShiftLeft(toUInt64(intDiv(number, 3000) % 5), 32) AS k, count(), sum(number), uniqExact(number % 1000), groupBitmap(toUInt32(number)) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT bitShiftLeft(toUInt64(intDiv(number, 3000) % 5), 32) AS k, count(), sum(number), uniqExact(number % 1000), groupBitmap(toUInt32(number)) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 0))
    UNION ALL
    (SELECT * FROM (SELECT bitShiftLeft(toUInt64(intDiv(number, 3000) % 5), 32) AS k, count(), sum(number), uniqExact(number % 1000), groupBitmap(toUInt32(number)) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT bitShiftLeft(toUInt64(intDiv(number, 3000) % 5), 32) AS k, count(), sum(number), uniqExact(number % 1000), groupBitmap(toUInt32(number)) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 1))
);

SELECT 'UInt8 key';
SELECT count() FROM
(
    (SELECT * FROM (SELECT toUInt8(intDiv(number, 3000) % 200) AS k, count(), sum(number) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT toUInt8(intDiv(number, 3000) % 200) AS k, count(), sum(number) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 0))
    UNION ALL
    (SELECT * FROM (SELECT toUInt8(intDiv(number, 3000) % 200) AS k, count(), sum(number) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT toUInt8(intDiv(number, 3000) % 200) AS k, count(), sum(number) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 1))
);

SELECT 'Date key';
SELECT count() FROM
(
    (SELECT * FROM (SELECT toDate('2020-01-01') + intDiv(number, 3000) AS k, count(), sum(number) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT toDate('2020-01-01') + intDiv(number, 3000) AS k, count(), sum(number) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 0))
    UNION ALL
    (SELECT * FROM (SELECT toDate('2020-01-01') + intDiv(number, 3000) AS k, count(), sum(number) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT toDate('2020-01-01') + intDiv(number, 3000) AS k, count(), sum(number) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 1))
);

SELECT 'Float64 key with -0, 0 and NaN';
SELECT count() FROM
(
    (SELECT * FROM (SELECT reinterpretAsUInt64(k), c, s FROM (SELECT [-0., 0., nan, 1.5][intDiv(number, 3000) % 4 + 1] AS k, count() AS c, sum(number) AS s FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 1))
     EXCEPT ALL
     SELECT * FROM (SELECT reinterpretAsUInt64(k), c, s FROM (SELECT [-0., 0., nan, 1.5][intDiv(number, 3000) % 4 + 1] AS k, count() AS c, sum(number) AS s FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 0)))
    UNION ALL
    (SELECT * FROM (SELECT reinterpretAsUInt64(k), c, s FROM (SELECT [-0., 0., nan, 1.5][intDiv(number, 3000) % 4 + 1] AS k, count() AS c, sum(number) AS s FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 0))
     EXCEPT ALL
     SELECT * FROM (SELECT reinterpretAsUInt64(k), c, s FROM (SELECT [-0., 0., nan, 1.5][intDiv(number, 3000) % 4 + 1] AS k, count() AS c, sum(number) AS s FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 1)))
);

SELECT 'FixedString key';
SELECT count() FROM
(
    (SELECT * FROM (SELECT toFixedString(leftPad(toString(intDiv(number, 3000) % 50), 4, '0'), 4) AS k, count(), sum(number) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT toFixedString(leftPad(toString(intDiv(number, 3000) % 50), 4, '0'), 4) AS k, count(), sum(number) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 0))
    UNION ALL
    (SELECT * FROM (SELECT toFixedString(leftPad(toString(intDiv(number, 3000) % 50), 4, '0'), 4) AS k, count(), sum(number) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT toFixedString(leftPad(toString(intDiv(number, 3000) % 50), 4, '0'), 4) AS k, count(), sum(number) FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 1))
);

SELECT 'no aggregate functions';
SELECT count() FROM
(
    (SELECT * FROM (SELECT bitShiftLeft(toUInt64(intDiv(number, 3000) % 5), 32) AS k FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT bitShiftLeft(toUInt64(intDiv(number, 3000) % 5), 32) AS k FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 0))
    UNION ALL
    (SELECT * FROM (SELECT bitShiftLeft(toUInt64(intDiv(number, 3000) % 5), 32) AS k FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT bitShiftLeft(toUInt64(intDiv(number, 3000) % 5), 32) AS k FROM numbers(300000) GROUP BY k SETTINGS max_block_size = 20000, optimize_group_by_constant_keys = 1))
);

SELECT 'frozen adaptive tables';
SELECT count() FROM
(
    (SELECT * FROM (SELECT intDiv(number, 2000) AS k, count(), sum(number) FROM numbers_mt(1000000) GROUP BY k SETTINGS max_threads = 4, max_block_size = 8192, enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0, optimize_group_by_constant_keys = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT intDiv(number, 2000) AS k, count(), sum(number) FROM numbers_mt(1000000) GROUP BY k SETTINGS max_threads = 4, max_block_size = 8192, enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0, optimize_group_by_constant_keys = 0))
    UNION ALL
    (SELECT * FROM (SELECT intDiv(number, 2000) AS k, count(), sum(number) FROM numbers_mt(1000000) GROUP BY k SETTINGS max_threads = 4, max_block_size = 8192, enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0, optimize_group_by_constant_keys = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT intDiv(number, 2000) AS k, count(), sum(number) FROM numbers_mt(1000000) GROUP BY k SETTINGS max_threads = 4, max_block_size = 8192, enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0, optimize_group_by_constant_keys = 1))
);

SELECT 'frozen adaptive tables, count only';
SELECT count() FROM
(
    (SELECT * FROM (SELECT intDiv(number, 2000) AS k, count() FROM numbers_mt(1000000) GROUP BY k SETTINGS max_threads = 4, max_block_size = 8192, enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0, optimize_group_by_constant_keys = 1)
     EXCEPT ALL
     SELECT * FROM (SELECT intDiv(number, 2000) AS k, count() FROM numbers_mt(1000000) GROUP BY k SETTINGS max_threads = 4, max_block_size = 8192, enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0, optimize_group_by_constant_keys = 0))
    UNION ALL
    (SELECT * FROM (SELECT intDiv(number, 2000) AS k, count() FROM numbers_mt(1000000) GROUP BY k SETTINGS max_threads = 4, max_block_size = 8192, enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0, optimize_group_by_constant_keys = 0)
     EXCEPT ALL
     SELECT * FROM (SELECT intDiv(number, 2000) AS k, count() FROM numbers_mt(1000000) GROUP BY k SETTINGS max_threads = 4, max_block_size = 8192, enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0, optimize_group_by_constant_keys = 1))
);

-- Part `p` holds keys `4p` to `4p + 20` in runs of 10000 rows, so `GROUP BY k LIMIT 3` refuses new keys in the middle
-- of the streams; every returned key must carry the values of the full `GROUP BY`.
SELECT 'GROUP BY LIMIT keeps exact keys';
DROP TABLE IF EXISTS t_key_runs_kept;
CREATE TABLE t_key_runs_kept (p UInt8, k UInt32, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY k SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO t_key_runs_kept SELECT 0, intDiv(number, 10000), number FROM numbers(210000) SETTINGS max_insert_block_size = 10000000, min_insert_block_size_rows = 10000000, min_insert_block_size_bytes = 0;
INSERT INTO t_key_runs_kept SELECT 1, 4 + intDiv(number, 10000), number + 1000000 FROM numbers(210000) SETTINGS max_insert_block_size = 10000000, min_insert_block_size_rows = 10000000, min_insert_block_size_bytes = 0;
INSERT INTO t_key_runs_kept SELECT 2, 8 + intDiv(number, 10000), number + 2000000 FROM numbers(210000) SETTINGS max_insert_block_size = 10000000, min_insert_block_size_rows = 10000000, min_insert_block_size_bytes = 0;
INSERT INTO t_key_runs_kept SELECT 3, 12 + intDiv(number, 10000), number + 3000000 FROM numbers(210000) SETTINGS max_insert_block_size = 10000000, min_insert_block_size_rows = 10000000, min_insert_block_size_bytes = 0;
SELECT count(), countIf(lim.c != f.c OR lim.s != f.s OR lim.b != f.b)
FROM (SELECT k, count() AS c, sum(v) AS s, groupBitmap(v) AS b FROM t_key_runs_kept GROUP BY k LIMIT 3) AS lim
INNER JOIN (SELECT k, count() AS c, sum(v) AS s, groupBitmap(v) AS b FROM t_key_runs_kept GROUP BY k) AS f ON lim.k = f.k
SETTINGS enable_group_by_top_k_optimization = 0, max_threads = 4, max_block_size = 65536, optimize_trivial_group_by_limit_query = 1, exact_rows_before_limit = 0;
DROP TABLE t_key_runs_kept;

-- Blocks of several keys are aggregated in batches of one place per run only with the setting on; with it off they
-- take one place per row.
SELECT 'several runs per block, one place per run';
SELECT bitShiftLeft(toUInt64(intDiv(number, 3000) % 5), 32) AS k, count(), sum(number) FROM numbers(300000) GROUP BY k
SETTINGS max_block_size = 20000, enable_adaptive_aggregator = 0, optimize_group_by_constant_keys = 1, log_comment = '05486_key_runs_on' FORMAT Null;
SELECT bitShiftLeft(toUInt64(intDiv(number, 3000) % 5), 32) AS k, count(), sum(number) FROM numbers(300000) GROUP BY k
SETTINGS max_block_size = 20000, enable_adaptive_aggregator = 0, optimize_group_by_constant_keys = 0, log_comment = '05486_key_runs_off' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['AggregationOptimizedEqualRangesOfKeys'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish' AND current_database = currentDatabase()
    AND log_comment IN ('05486_key_runs_on', '05486_key_runs_off')
ORDER BY log_comment;
