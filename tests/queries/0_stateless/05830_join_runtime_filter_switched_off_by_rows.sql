-- A JOIN runtime filter is switched off for some blocks when it filters out too few rows for the lookups that it
-- takes, see `join_runtime_filter_pass_ratio_threshold_for_disabling`. A plain key takes a lookup per row, so its
-- filter is switched off when more than 70% of the rows pass. A `LowCardinality` key takes a lookup per value of the
-- dictionary, 11 for each block here, so its filter stays on while it filters out more than 0.3 rows per lookup. The
-- share of the dictionary that passes says nothing about the rows, and does not count.

SET enable_analyzer = 1, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, join_runtime_filter_exact_values_limit = 100;
SET join_algorithm = 'hash', query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET join_runtime_filter_from_fixed_hash_table = 0, enable_join_runtime_filters_index_analysis = 0;
SET join_runtime_filter_pass_ratio_threshold_for_disabling = 0.7, join_runtime_filter_blocks_to_skip_before_reenabling = 30;
-- One thread reads several blocks one after another, so a filter that the first block switches off skips the next ones.
SET max_threads = 1, max_block_size = 8192;

DROP TABLE IF EXISTS t_probe;
DROP TABLE IF EXISTS t_build_rare;
DROP TABLE IF EXISTS t_build_frequent;

CREATE TABLE t_probe (k LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_build_rare (k LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_build_frequent (k LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();

-- 91% of the rows have one value, and each of the other 9 values is in 1% of the rows. With the default the dictionary
-- has 11 values.
INSERT INTO t_probe SELECT if(number % 100 < 91, 'frequent', concat('rare', toString(number % 100 - 90))) FROM numbers(100000);
INSERT INTO t_build_rare SELECT concat('rare', toString(number + 1)) FROM numbers(9);
INSERT INTO t_build_frequent VALUES ('frequent');

-- 9% of the rows pass, and 9 of the 11 values of the dictionary: the filter stays on.
SELECT count() FROM t_probe AS p INNER JOIN t_build_rare AS b ON p.k = b.k
SETTINGS log_comment = 'few rows pass';
-- 91% of the rows pass, and 1 of the 11 values of the dictionary: the filter stays on, the other 9% of the rows are
-- worth 11 lookups.
SELECT count() FROM t_probe AS p INNER JOIN t_build_frequent AS b ON p.k = b.k
SETTINGS log_comment = 'most rows pass';
-- The same rows of a plain key are not worth a lookup each: the filter is switched off.
SELECT count() FROM (SELECT CAST(k, 'String') AS k FROM t_probe) AS p INNER JOIN (SELECT CAST(k, 'String') AS k FROM t_build_frequent) AS b ON p.k = b.k
SETTINGS log_comment = 'most rows pass, plain key';
-- Every row passes: the filter is switched off.
SELECT count() FROM t_probe AS p INNER JOIN (SELECT k FROM t_build_rare UNION ALL SELECT k FROM t_build_frequent) AS b ON p.k = b.k
SETTINGS log_comment = 'all rows pass';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['RuntimeFilterRowsChecked'] + ProfileEvents['RuntimeFilterRowsSkipped'] AS checked_or_skipped,
    ProfileEvents['RuntimeFilterRowsSkipped'] > 0 AS switched_off
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
    AND current_database = currentDatabase() AND log_comment LIKE '%rows pass%'
ORDER BY log_comment;

DROP TABLE t_probe;
DROP TABLE t_build_rare;
DROP TABLE t_build_frequent;
