-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: checks for a hit in the instance-wide query condition cache, which other tests clear.
-- Tag no-parallel-replicas: parallel replicas move the cache writes to other replicas.

-- A query with FINAL uses the query condition cache if its condition uses only the columns
-- identical in all versions of a row (see `columns_identical_across_versions`).

SET optimize_move_to_prewhere = 1, optimize_move_to_prewhere_if_final = 1, apply_prewhere_after_final = 0;

DROP TABLE IF EXISTS t_identical;

CREATE TABLE t_identical (id UInt64, b UInt64, version UInt64)
ENGINE = ReplacingMergeTree(version) ORDER BY id
SETTINGS columns_identical_across_versions = 'b', add_minmax_index_for_numeric_columns = 0,
    index_granularity = 8192, index_granularity_bytes = 10485760;

SYSTEM STOP MERGES t_identical;

INSERT INTO t_identical SELECT number, intDiv(number, 3), 1 FROM numbers(1000000);
INSERT INTO t_identical SELECT number, intDiv(number, 3), 2 FROM numbers(500000);

SELECT sum(id) FROM t_identical FINAL WHERE b = 10000 SETTINGS use_query_condition_cache = 1, log_comment = '05482_first';
SELECT sum(id) FROM t_identical FINAL WHERE b = 10000 SETTINGS use_query_condition_cache = 1, log_comment = '05482_second';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
    AND log_comment IN ('05482_first', '05482_second')
ORDER BY log_comment;

DROP TABLE t_identical;
