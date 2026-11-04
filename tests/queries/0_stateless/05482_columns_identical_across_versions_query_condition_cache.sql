-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: checks for a hit in the instance-wide query condition cache, which other tests clear.
-- Tag no-parallel-replicas: parallel replicas move the cache writes to other replicas.

-- A query with FINAL uses the query condition cache if its condition uses only the columns
-- identical in all versions of a row (see `columns_identical_across_versions`).

SET optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1, apply_prewhere_after_final = 0;

DROP TABLE IF EXISTS t_identical;

CREATE TABLE t_identical (id UInt64, b UInt64, s UInt64, version UInt64)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS columns_identical_across_versions = 'b', add_minmax_index_for_numeric_columns = 0,
    index_granularity = 8192, index_granularity_bytes = 10485760;

SYSTEM STOP MERGES t_identical;

INSERT INTO t_identical SELECT number, intDiv(number, 3), number, 1 FROM numbers(1000000);
INSERT INTO t_identical SELECT number, intDiv(number, 3), number + 1000000, 2 FROM numbers(500000);

SELECT sum(id) FROM t_identical FINAL WHERE b = 10000 SETTINGS use_query_condition_cache = 1, log_comment = '05482_first';
SELECT sum(id) FROM t_identical FINAL WHERE b = 10000 SETTINGS use_query_condition_cache = 1, log_comment = '05482_second';

-- The entry for the varying column `s` written without FINAL says that the new versions do not match `s = 10000`,
-- but with FINAL they replace the matching old version, so the query must not use it.
SELECT count() FROM t_identical WHERE s = 10000 SETTINGS use_query_condition_cache = 1;
SELECT count() FROM t_identical FINAL WHERE s = 10000 SETTINGS use_query_condition_cache = 1, log_comment = '05482_varying';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
    AND log_comment IN ('05482_first', '05482_second', '05482_varying')
ORDER BY log_comment;

DROP TABLE t_identical;

-- The condition of `indexHint` is not among the arguments of the function, but the index on the version column uses it.
-- The entry written without FINAL drops the new version of the row 1, so the query with FINAL must not use it.
DROP TABLE IF EXISTS t_index_hint;
CREATE TABLE t_index_hint (id UInt64, s UInt64, v UInt64, INDEX idx_v v TYPE minmax GRANULARITY 1)
ENGINE = ReplacingMergeTree(v) ORDER BY id
SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
SYSTEM STOP MERGES t_index_hint;
INSERT INTO t_index_hint VALUES (1, 100, 1);
INSERT INTO t_index_hint VALUES (1, 200, 2), (2, 300, 1);

SET optimize_move_to_prewhere = 0, use_skip_indexes = 1, use_skip_indexes_if_final = 1,
    use_skip_indexes_on_data_read = 0, use_skip_indexes_if_final_exact_mode = 1, use_query_condition_cache = 1;
SELECT arraySort(groupArray((id, s))) FROM t_index_hint WHERE indexHint(v = 1);
SELECT arraySort(groupArray((id, s))) FROM t_index_hint FINAL WHERE indexHint(v = 1);

DROP TABLE t_index_hint;
