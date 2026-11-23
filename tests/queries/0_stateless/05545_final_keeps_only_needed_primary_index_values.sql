-- Tags: no-random-merge-tree-settings, no-parallel-replicas, no-replicated-database
-- no-random-merge-tree-settings: the test depends on granules and on how the primary index is loaded
-- no-parallel-replicas, no-replicated-database: the test checks profile events of the query

SET use_skip_indexes_if_final = 1, use_skip_indexes_if_final_exact_mode = 1, use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0;
SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 1, split_intersecting_parts_ranges_into_layers_final = 1;
SET optimize_on_insert = 0, max_threads = 4, max_final_threads = 4;

DROP TABLE IF EXISTS t;

-- 10 parts with 100 granules each.
CREATE TABLE t (k String, v UInt64, s UInt64, INDEX i s TYPE minmax GRANULARITY 1)
ENGINE = ReplacingMergeTree(v) ORDER BY k
SETTINGS index_granularity = 1, use_primary_key_cache = 1, primary_key_lazy_load = 1, max_bytes_to_merge_at_max_space_in_pool = 1;

SYSTEM STOP MERGES t;

INSERT INTO t SELECT toString(cityHash64(number % 750)), number, number % 750 FROM numbers(1000)
SETTINGS max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, max_insert_threads = 1, max_threads = 1;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;

SELECT count(), sum(v) FROM t FINAL WHERE s = 5;
SELECT count(), sum(v) FROM t FINAL WHERE s BETWEEN 240 AND 260;
SELECT count(), sum(v) FROM t FINAL WHERE s < 100 SETTINGS split_intersecting_parts_ranges_into_layers_final = 0;
SELECT count(), sum(v) FROM t FINAL WHERE s < 100 SETTINGS split_parts_ranges_into_intersecting_and_non_intersecting_final = 0;
SELECT count(), sum(v) FROM t FINAL WHERE s < 100 AND k > '5';
SELECT count(), sum(v) FROM t FINAL;
-- The same without `FINAL`.
SELECT count(), sum(v) FROM (SELECT k, max(v) AS v, any(s) AS s FROM t GROUP BY k) WHERE s = 5;
SELECT count(), sum(v) FROM (SELECT k, max(v) AS v, any(s) AS s FROM t GROUP BY k) WHERE s BETWEEN 240 AND 260;
SELECT count(), sum(v) FROM (SELECT k, max(v) AS v, any(s) AS s FROM t GROUP BY k) WHERE s < 100;
SELECT count(), sum(v) FROM (SELECT k, max(v) AS v, any(s) AS s FROM t GROUP BY k) WHERE s < 100 AND k > '5';
SELECT count(), sum(v) FROM (SELECT k, max(v) AS v, any(s) AS s FROM t GROUP BY k);

-- No index is loaded if there is nothing to read.
DETACH TABLE t;
ATTACH TABLE t;

SELECT count() FROM t FINAL WHERE s = 2000 SETTINGS log_comment = '05545_nothing';

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['LoadedPrimaryIndexFiles'] FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05545_nothing' AND type = 'QueryFinish' AND event_date >= yesterday();

DROP TABLE t;
