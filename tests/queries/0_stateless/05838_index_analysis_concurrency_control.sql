-- The threads analyzing indexes take CPU slots of the concurrency control.

SET ignore_format_null_for_explain=0;

DROP TABLE IF EXISTS t_index_analysis_cc;

CREATE TABLE t_index_analysis_cc (key UInt64, value UInt64, INDEX idx_value value TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY key SETTINGS index_granularity = 8;

SYSTEM STOP MERGES t_index_analysis_cc;
INSERT INTO t_index_analysis_cc SELECT number, number FROM numbers(0, 100);
INSERT INTO t_index_analysis_cc SELECT number, number FROM numbers(100, 100);
INSERT INTO t_index_analysis_cc SELECT number, number FROM numbers(200, 100);
INSERT INTO t_index_analysis_cc SELECT number, number FROM numbers(300, 100);

-- `EXPLAIN` analyzes indexes without executing the read, so only the analysis and the pipeline of `EXPLAIN` itself take slots.
SET max_threads = 4;

SET max_threads_for_indexes = 1, use_concurrency_control = 1, log_comment = 'serial';
EXPLAIN indexes = 1 SELECT value FROM t_index_analysis_cc WHERE key > 10 AND value < 390 FORMAT Null;

SET max_threads_for_indexes = 4, use_concurrency_control = 1, log_comment = 'parallel';
EXPLAIN indexes = 1 SELECT value FROM t_index_analysis_cc WHERE key > 10 AND value < 390 FORMAT Null;

SET max_threads_for_indexes = 4, use_concurrency_control = 0, log_comment = 'parallel without concurrency control';
EXPLAIN indexes = 1 SELECT value FROM t_index_analysis_cc WHERE key > 10 AND value < 390 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

WITH (
    SELECT groupArray((log_comment, ProfileEvents['ConcurrencyControlSlotsAcquired'] + ProfileEvents['ConcurrencyControlSlotsAcquiredNonCompeting']))
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Explain'
) AS slots
SELECT
    -- The parallel analysis takes at least the slot of its calling thread, the serial analysis takes none.
    arrayFirst(x -> x.1 = 'parallel', slots).2 > arrayFirst(x -> x.1 = 'serial', slots).2,
    arrayFirst(x -> x.1 = 'parallel without concurrency control', slots).2;

DROP TABLE t_index_analysis_cc;
