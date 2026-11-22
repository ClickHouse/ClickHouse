-- A multi-granule read of a Compact part must not read granules emptied by an earlier PREWHERE step again,
-- even when they are less than half of the read.

CREATE TABLE t (k UInt32, a UInt8, b UInt8, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000, index_granularity = 100, index_granularity_bytes = '10M';

-- a = 0 in granules 0, 3, 6 and in the last one (50 rows): 350 of 950 rows.
INSERT INTO t SELECT number, intDiv(number, 100) % 3 != 0, number % 2, number FROM numbers(950)
SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, max_insert_threads = 1;

SELECT part_type, rows FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active;
SELECT groupArray(rows_in_granule) FROM (SELECT rows_in_granule FROM mergeTreeIndex(currentDatabase(), t) ORDER BY mark_number);

SELECT sum(v) FROM t PREWHERE a = 1 AND b = 0
SETTINGS max_block_size = 1000, preferred_block_size_bytes = 0, enable_multiple_prewhere_read_steps = 1, max_threads = 1,
    use_query_condition_cache = 0, enable_parallel_replicas = 0, log_queries = 1, log_comment = '05632_one_read';

SELECT sum(v) FROM t PREWHERE a = 1 AND b = 0
SETTINGS enable_multiple_prewhere_read_steps = 1, max_threads = 1,
    use_query_condition_cache = 0, enable_parallel_replicas = 0, log_queries = 1, log_comment = '05632_predictor';

SELECT sum(v) FROM t WHERE a = 1 AND b = 0 SETTINGS optimize_move_to_prewhere = 0;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['RowsReadByPrewhereReaders'], ProfileEvents['RowsReadByMainReader'] FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05632_one_read', '05632_predictor')
ORDER BY log_comment;

DROP TABLE t;
