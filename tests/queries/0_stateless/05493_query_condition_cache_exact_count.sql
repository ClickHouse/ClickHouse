-- Tags: no-parallel-replicas
-- no-parallel-replicas: reads with parallel replicas neither record nor use match counts
-- Random settings limits: use_query_condition_cache=(1, 1); optimize_move_to_prewhere=(1, 1); optimize_use_projections=(1, 1); optimize_use_implicit_projections=(1, 1); apply_string_filters_during_scan=(1, 1)

-- A count() whose whole filter is a deterministic PREWHERE records the exact number of matching rows
-- per granule in the query condition cache, and a later count() with the same filter adds them up
-- instead of reading those granules again. Check that the counts are used, and that they are never
-- used for another filter, another part, or a read that returns other rows. The queries tagged with
-- `log_comment` are classified at the end: 'counted' (answered from the cache alone, `read_rows` is the
-- one row of the prepared count), 'partly counted' (some granules counted, the others read) or 'read'.
-- Merges are stopped wherever the parts must stay the same between two queries.

-- apply_string_filters_during_scan moves the string filters of a count() to PREWHERE, and only filters in PREWHERE are
-- counted.
SET apply_mutations_on_fly = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_qcc_count;
DROP TABLE IF EXISTS t_qcc_count_parts;
DROP TABLE IF EXISTS t_qcc_count_adaptive;
DROP TABLE IF EXISTS t_qcc_count_final;
DROP TABLE IF EXISTS t_qcc_count_sample;

-- 101 granules of 1000 rows, except the last one with 500 rows. 'match' is only in the first 50 granules.
CREATE TABLE t_qcc_count (k UInt64, v UInt64, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1000, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_qcc_count;

INSERT INTO t_qcc_count
SELECT number, (number * 7919) % 100003, if(number % 7 = 0 AND number < 50000, concat('match', toString(number)), toString(number))
FROM numbers(100500);

SELECT '-- the second count() is answered from the counts recorded by the first';
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_01';
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_02';
-- Not only a count() of this filter: read as before
SELECT count(), sum(length(s)) FROM t_qcc_count WHERE s LIKE '%match%';
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' AND v > 50000 SETTINGS log_comment = 'qcc_count_03';
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS use_query_condition_cache = 0, log_comment = 'qcc_count_04';

SELECT '-- other constants';
SELECT count() FROM t_qcc_count WHERE s LIKE '%match1%' SETTINGS log_comment = 'qcc_count_05';
SELECT count() FROM t_qcc_count WHERE s LIKE '%match1%' SETTINGS log_comment = 'qcc_count_06';
-- Counts recorded for one bound are not used for another one, narrower or wider
SELECT count() FROM t_qcc_count PREWHERE v > 30000 SETTINGS log_comment = 'qcc_count_07';
SELECT count() FROM t_qcc_count PREWHERE v > 60000 SETTINGS log_comment = 'qcc_count_08';
SELECT count() FROM t_qcc_count PREWHERE v > 20000 SETTINGS log_comment = 'qcc_count_09';
SELECT count() FROM t_qcc_count PREWHERE v > 30000 SETTINGS log_comment = 'qcc_count_10';
SELECT count() FROM t_qcc_count PREWHERE v > 30000 SETTINGS use_query_condition_cache = 0;

SELECT '-- count(expr) evaluates its argument, so only a bare count() is answered from the counts';
SELECT count(throwIf(k >= 0)) FROM t_qcc_count PREWHERE v > 30000; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT count(*) FROM t_qcc_count PREWHERE v > 30000 SETTINGS log_comment = 'qcc_count_37';

SELECT '-- non-deterministic filters are neither counted nor answered from counts';
SELECT count() BETWEEN 1 AND 7142 FROM t_qcc_count WHERE s LIKE '%match%' AND rand() % 2 = 0 SETTINGS log_comment = 'qcc_count_11';
SELECT count() BETWEEN 1 AND 7142 FROM t_qcc_count WHERE s LIKE '%match%' AND rand() % 2 = 0 SETTINGS log_comment = 'qcc_count_12';
-- randConstant() is the same within a query but changes between queries
SELECT count() IN (1587, 158) FROM t_qcc_count WHERE s LIKE if(randConstant() % 2 = 0, '%match1%', '%match5%') SETTINGS log_comment = 'qcc_count_13';
SELECT count() IN (1587, 158) FROM t_qcc_count WHERE s LIKE if(randConstant() % 2 = 0, '%match1%', '%match5%') SETTINGS log_comment = 'qcc_count_14';

SELECT '-- a read stopped early records counts only for the read tasks it finished';
CREATE TABLE t_qcc_count_parts (k UInt64, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1000, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_qcc_count_parts;
INSERT INTO t_qcc_count_parts SELECT number, if(number % 3 = 0, 'match', 'x') FROM numbers(0, 25000);
INSERT INTO t_qcc_count_parts SELECT number, if(number % 3 = 0, 'match', 'x') FROM numbers(25000, 25000);
INSERT INTO t_qcc_count_parts SELECT number, if(number % 3 = 0, 'match', 'x') FROM numbers(50000, 25000);
INSERT INTO t_qcc_count_parts SELECT number, if(number % 3 = 0, 'match', 'x') FROM numbers(75000, 25000);
SELECT count() FROM t_qcc_count_parts WHERE s LIKE '%match%'
SETTINGS max_rows_to_read = 30000, read_overflow_mode = 'break', max_threads = 1, log_comment = 'qcc_count_15' FORMAT Null;
SELECT count() FROM t_qcc_count_parts WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_16';
SELECT count() FROM t_qcc_count_parts WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_17';

SELECT '-- granules of different sizes';
CREATE TABLE t_qcc_count_adaptive (k UInt64, s String)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = 10240, add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_qcc_count_adaptive;
INSERT INTO t_qcc_count_adaptive SELECT number, concat(repeat('x', number % 97), if(number % 5 = 0, 'needle', '')) FROM numbers(100003);
SELECT count() FROM t_qcc_count_adaptive WHERE s LIKE '%needle%' SETTINGS log_comment = 'qcc_count_18';
SELECT count() FROM t_qcc_count_adaptive WHERE s LIKE '%needle%' SETTINGS log_comment = 'qcc_count_19';

SELECT '-- inserts, merges and mutations';
INSERT INTO t_qcc_count
SELECT number, (number * 7919) % 100003, if(number % 7 = 0, concat('match', toString(number)), toString(number))
FROM numbers(100500, 20000);
-- Only the new part is read
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_20';
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_21';
SYSTEM START MERGES t_qcc_count;
OPTIMIZE TABLE t_qcc_count FINAL;
SYSTEM STOP MERGES t_qcc_count;
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_22';
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_23';
SYSTEM START MERGES t_qcc_count;
ALTER TABLE t_qcc_count UPDATE s = 'none' WHERE k % 14 = 0 SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES t_qcc_count;
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_24';
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_25';
-- A pending mutation applied on the fly: counts are neither used nor recorded
ALTER TABLE t_qcc_count UPDATE s = 'none' WHERE k % 28 = 7 SETTINGS mutations_sync = 0;
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS apply_mutations_on_fly = 1, log_comment = 'qcc_count_26';
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS apply_mutations_on_fly = 1, log_comment = 'qcc_count_27';
-- Without applying it, the earlier counts of the unchanged part are still right
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS apply_mutations_on_fly = 0, log_comment = 'qcc_count_28';
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS use_query_condition_cache = 0, apply_mutations_on_fly = 1;
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS use_query_condition_cache = 0, apply_mutations_on_fly = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_qcc_count' SYNC FORMAT Null;
SYSTEM START MERGES t_qcc_count;
-- Lightweight deletes: the table no longer uses the exact count optimization, the results stay right
DELETE FROM t_qcc_count WHERE k % 21 = 0 SETTINGS lightweight_deletes_sync = 2;
SYSTEM STOP MERGES t_qcc_count;
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_29';
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_30';
SELECT count() FROM t_qcc_count WHERE s LIKE '%match%' SETTINGS use_query_condition_cache = 0;

SELECT '-- FINAL';
CREATE TABLE t_qcc_count_final (k UInt64, s String)
ENGINE = ReplacingMergeTree ORDER BY k
SETTINGS index_granularity = 1000, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_qcc_count_final;
INSERT INTO t_qcc_count_final SELECT number, if(number % 3 = 0, 'match', 'x') FROM numbers(50000);
INSERT INTO t_qcc_count_final SELECT number, if(number % 3 = 0, 'match', 'x') FROM numbers(25000);
SELECT count() FROM t_qcc_count_final WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_31';
SELECT count() FROM t_qcc_count_final WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_32';
SELECT count() FROM t_qcc_count_final FINAL WHERE s LIKE '%match%' SETTINGS optimize_move_to_prewhere_if_final = 1;
SELECT count() FROM t_qcc_count_final FINAL WHERE s LIKE '%match%' SETTINGS optimize_move_to_prewhere_if_final = 1;
SELECT count() FROM t_qcc_count_final WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_33';

SELECT '-- SAMPLE';
CREATE TABLE t_qcc_count_sample (k UInt64, s String)
ENGINE = MergeTree ORDER BY intHash32(k) SAMPLE BY intHash32(k)
SETTINGS index_granularity = 1000, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_qcc_count_sample;
INSERT INTO t_qcc_count_sample SELECT number, if(number % 3 = 0, 'match', 'x') FROM numbers(100000);
SELECT count() FROM t_qcc_count_sample WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_34';
SELECT count() FROM t_qcc_count_sample WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_35';
SELECT count() FROM t_qcc_count_sample SAMPLE 1 / 2 WHERE s LIKE '%match%';
SELECT count() FROM t_qcc_count_sample SAMPLE 1 / 2 WHERE s LIKE '%match%' SETTINGS use_query_condition_cache = 0;
SELECT count() FROM t_qcc_count_sample WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_count_36';

SELECT '-- counted from the cache';
SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    multiIf(NOT has(projections, tables[1] || '._exact_count_projection'), 'read', read_rows = 1, 'counted', 'partly counted')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 1200 AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND log_comment LIKE 'qcc\\_count\\_%' AND log_comment != 'qcc_count_15'
ORDER BY log_comment;

DROP TABLE t_qcc_count;
DROP TABLE t_qcc_count_parts;
DROP TABLE t_qcc_count_adaptive;
DROP TABLE t_qcc_count_final;
DROP TABLE t_qcc_count_sample;
