-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: drops the (instance-wide) query condition cache
-- no-parallel-replicas: reads with parallel replicas neither record nor use match counts

-- The exact counts of matching rows per granule that the query condition cache keeps for a count() are
-- keyed by part name and condition. A metadata-only ALTER can change what an unchanged part returns: a
-- column added after the part was written is computed from its current DEFAULT, and pending RENAME COLUMN,
-- DROP COLUMN and type changes are applied while reading. Counts are then neither used nor recorded for
-- such parts, while parts that store the columns keep using theirs (issue #123952 for the matching marks).
-- Every answer is followed by the answer without the cache. The queries tagged with `log_comment` are
-- classified at the end: 'counted' (answered from the cache alone), 'partly counted' (some granules
-- counted, the others read) or 'read'.

SET use_query_condition_cache = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
SET apply_mutations_on_fly = 0;
SET enable_parallel_replicas = 0;
SET session_timezone = 'UTC';
-- Moves the string filters of a count() to PREWHERE: only filters in PREWHERE are counted
SET apply_string_filters_during_scan = 1;
-- Reads through the columns cache do not record counts
SET use_columns_cache = 0;
-- Part pruning by statistics and the trivial count over them do not see a pending DROP COLUMN
SET use_statistics_for_part_pruning = 0;
SET optimize_trivial_count_with_sparsity_filter = 0;

DROP TABLE IF EXISTS t_qcc_count_alter;

CREATE TABLE t_qcc_count_alter (k UInt64, s String, v UInt32, d DateTime('UTC'))
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1000, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0,
    add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_qcc_count_alter;

INSERT INTO t_qcc_count_alter SELECT number, if(number % 3 = 0, 'match', 'x'), number % 1000, '2020-01-01 00:00:00' FROM numbers(100000);
SYSTEM DROP QUERY CONDITION CACHE;

SELECT '-- the DEFAULT of a column that the first part does not store changes';
ALTER TABLE t_qcc_count_alter ADD COLUMN y String DEFAULT 'match';
-- The second part stores y. Every granule has rows matching each filter below, so that the matching marks of the
-- query condition cache do not skip granules whose rows match after an ALTER (issue #123952).
INSERT INTO t_qcc_count_alter (k, s, v, d, y)
SELECT number, if(number % 2 = 0, 'match', 'x'), number % 1000, '2020-01-01 00:00:00', if(number % 4 = 0, 'match', 'y') FROM numbers(100000, 20000);
SELECT count() FROM t_qcc_count_alter WHERE y LIKE '%match%' SETTINGS log_comment = 'qcc_alter_01';
SELECT count() FROM t_qcc_count_alter WHERE y LIKE '%match%' SETTINGS log_comment = 'qcc_alter_02';
ALTER TABLE t_qcc_count_alter MODIFY COLUMN y String DEFAULT 'none';
SELECT count() FROM t_qcc_count_alter WHERE y LIKE '%match%' SETTINGS log_comment = 'qcc_alter_03';
SELECT count() FROM t_qcc_count_alter WHERE y LIKE '%match%' SETTINGS use_query_condition_cache = 0;
-- Not tested: a DEFAULT that makes more rows of the first part match again. The matching marks of the first part
-- record that it has no match by now, and they are not part of this change (issue #123952).
ALTER TABLE t_qcc_count_alter MODIFY COLUMN y REMOVE DEFAULT;
SELECT count() FROM t_qcc_count_alter WHERE y LIKE '%match%' SETTINGS log_comment = 'qcc_alter_04';
SELECT count() FROM t_qcc_count_alter WHERE y LIKE '%match%' SETTINGS use_query_condition_cache = 0;

SELECT '-- a DEFAULT computed from another column: a positive count becomes smaller';
ALTER TABLE t_qcc_count_alter ADD COLUMN z UInt64 DEFAULT k % 2;
SELECT count() FROM t_qcc_count_alter PREWHERE z = 1 SETTINGS log_comment = 'qcc_alter_05';
SELECT count() FROM t_qcc_count_alter PREWHERE z = 1 SETTINGS log_comment = 'qcc_alter_06';
ALTER TABLE t_qcc_count_alter MODIFY COLUMN z UInt64 DEFAULT k % 4;
SELECT count() FROM t_qcc_count_alter PREWHERE z = 1 SETTINGS log_comment = 'qcc_alter_07';
SELECT count() FROM t_qcc_count_alter PREWHERE z = 1 SETTINGS use_query_condition_cache = 0;

SELECT '-- the time zone of a column that a DEFAULT depends on changes, without a mutation: a positive count becomes zero';
ALTER TABLE t_qcc_count_alter ADD COLUMN t String DEFAULT toString(d);
SELECT count() FROM t_qcc_count_alter WHERE t LIKE '%00:00:00%' SETTINGS log_comment = 'qcc_alter_08';
SELECT count() FROM t_qcc_count_alter WHERE t LIKE '%00:00:00%' SETTINGS log_comment = 'qcc_alter_09';
ALTER TABLE t_qcc_count_alter MODIFY COLUMN d DateTime('Asia/Tokyo');
SELECT count() FROM t_qcc_count_alter WHERE t LIKE '%00:00:00%' SETTINGS log_comment = 'qcc_alter_10';
SELECT count() FROM t_qcc_count_alter WHERE t LIKE '%00:00:00%' SETTINGS use_query_condition_cache = 0;

SELECT '-- a non-deterministic DEFAULT';
ALTER TABLE t_qcc_count_alter ADD COLUMN r UInt64 DEFAULT rand64();
SELECT count() BETWEEN 1 AND 119999 FROM t_qcc_count_alter PREWHERE r % 2 = 0 SETTINGS log_comment = 'qcc_alter_11';
SELECT count() BETWEEN 1 AND 119999 FROM t_qcc_count_alter PREWHERE r % 2 = 0 SETTINGS log_comment = 'qcc_alter_12';

SELECT '-- a pending type change';
SELECT count() FROM t_qcc_count_alter PREWHERE v < 100 SETTINGS log_comment = 'qcc_alter_13';
SELECT count() FROM t_qcc_count_alter PREWHERE v < 100 SETTINGS log_comment = 'qcc_alter_14';
ALTER TABLE t_qcc_count_alter MODIFY COLUMN v UInt64 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT count() FROM t_qcc_count_alter PREWHERE v < 100 SETTINGS log_comment = 'qcc_alter_15';
SELECT count() FROM t_qcc_count_alter PREWHERE v < 100 SETTINGS log_comment = 'qcc_alter_16';
SELECT count() FROM t_qcc_count_alter PREWHERE v < 100 SETTINGS use_query_condition_cache = 0;
-- Apply the pending mutation (the parts get new names) and wait for it
SYSTEM START MERGES t_qcc_count_alter;
ALTER TABLE t_qcc_count_alter DELETE WHERE k > 1000000000 SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES t_qcc_count_alter;
SELECT count() FROM t_qcc_count_alter PREWHERE v < 100 SETTINGS log_comment = 'qcc_alter_17';
SELECT count() FROM t_qcc_count_alter PREWHERE v < 100 SETTINGS log_comment = 'qcc_alter_18';

SELECT '-- a pending RENAME COLUMN';
SELECT count() FROM t_qcc_count_alter WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_alter_19';
SELECT count() FROM t_qcc_count_alter WHERE s LIKE '%match%' SETTINGS log_comment = 'qcc_alter_20';
ALTER TABLE t_qcc_count_alter RENAME COLUMN s TO s2 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT count() FROM t_qcc_count_alter WHERE s2 LIKE '%match%' SETTINGS log_comment = 'qcc_alter_21';
SELECT count() FROM t_qcc_count_alter WHERE s2 LIKE '%match%' SETTINGS log_comment = 'qcc_alter_22';
SELECT count() FROM t_qcc_count_alter WHERE s2 LIKE '%match%' SETTINGS use_query_condition_cache = 0;
SYSTEM START MERGES t_qcc_count_alter;
ALTER TABLE t_qcc_count_alter DELETE WHERE k > 1000000000 SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES t_qcc_count_alter;
SELECT count() FROM t_qcc_count_alter WHERE s2 LIKE '%match%' SETTINGS log_comment = 'qcc_alter_23';
SELECT count() FROM t_qcc_count_alter WHERE s2 LIKE '%match%' SETTINGS log_comment = 'qcc_alter_24';

SELECT '-- a pending DROP COLUMN, the same column added again, then KILL MUTATION';
ALTER TABLE t_qcc_count_alter DROP COLUMN s2 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_qcc_count_alter ADD COLUMN s2 String DEFAULT 'match' SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT count() FROM t_qcc_count_alter WHERE s2 LIKE '%match%' SETTINGS log_comment = 'qcc_alter_25';
SELECT count() FROM t_qcc_count_alter WHERE s2 LIKE '%match%' SETTINGS log_comment = 'qcc_alter_26';
SELECT count() FROM t_qcc_count_alter WHERE s2 LIKE '%match%' SETTINGS use_query_condition_cache = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_qcc_count_alter' SYNC FORMAT Null;
SELECT count() FROM t_qcc_count_alter WHERE s2 LIKE '%match%' SETTINGS log_comment = 'qcc_alter_27';
SELECT count() FROM t_qcc_count_alter WHERE s2 LIKE '%match%' SETTINGS log_comment = 'qcc_alter_28';
SELECT count() FROM t_qcc_count_alter WHERE s2 LIKE '%match%' SETTINGS use_query_condition_cache = 0;

SELECT '-- a materialized column is counted again, and later DEFAULT changes do not affect it';
ALTER TABLE t_qcc_count_alter MODIFY COLUMN y String DEFAULT 'none';
SYSTEM START MERGES t_qcc_count_alter;
ALTER TABLE t_qcc_count_alter MATERIALIZE COLUMN y SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES t_qcc_count_alter;
SELECT count() FROM t_qcc_count_alter WHERE y LIKE '%match%' SETTINGS log_comment = 'qcc_alter_29';
SELECT count() FROM t_qcc_count_alter WHERE y LIKE '%match%' SETTINGS log_comment = 'qcc_alter_30';
ALTER TABLE t_qcc_count_alter MODIFY COLUMN y String DEFAULT 'match';
SELECT count() FROM t_qcc_count_alter WHERE y LIKE '%match%' SETTINGS log_comment = 'qcc_alter_31';
SELECT count() FROM t_qcc_count_alter WHERE y LIKE '%match%' SETTINGS use_query_condition_cache = 0;

SELECT '-- counted from the cache';
SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    multiIf(NOT has(projections, tables[1] || '._exact_count_projection'), 'read', read_rows = 1, 'counted', 'partly counted')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 1200 AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND log_comment LIKE 'qcc\\_alter\\_%'
ORDER BY log_comment;

DROP TABLE t_qcc_count_alter;
