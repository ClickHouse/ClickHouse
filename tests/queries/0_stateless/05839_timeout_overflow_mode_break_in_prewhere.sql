-- With `timeout_overflow_mode = 'break'` a query stops at `max_execution_time` and returns a partial result.
-- A function that watches the time limit (`countMatches` here) throws `TIMEOUT_EXCEEDED` when the deadline
-- passes inside it. In `WHERE` the executor stopped the query first; in `PREWHERE` the exception failed the
-- read task and the query with it. Both must return a partial result under `break`.
-- The work takes seconds and the limit is 0.5 s, so the deadline lands inside the function. The amount of
-- data read is machine-dependent, so only the absence of an error is checked.

-- No other limit may stop the queries, and the read must run in this query's own executor, not on parallel replicas.
SET max_rows_to_read = 0, max_estimated_execution_time = 0, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

DROP TABLE IF EXISTS t_break_prewhere;
CREATE TABLE t_break_prewhere (s String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8;
INSERT INTO t_break_prewhere SELECT 'a' FROM numbers(1000);

SELECT count() FROM t_break_prewhere PREWHERE countMatches(repeat(s, 1000000), 'a') > 0
SETTINGS max_execution_time = 0.5, timeout_overflow_mode = 'break', max_block_size = 8, max_threads = 1
FORMAT Null;

SELECT count() FROM t_break_prewhere WHERE countMatches(repeat(s, 1000000), 'a') > 0
SETTINGS max_execution_time = 0.5, timeout_overflow_mode = 'break', max_block_size = 8, max_threads = 1, optimize_move_to_prewhere = 0
FORMAT Null;

-- `throw` still fails.
SELECT count() FROM t_break_prewhere PREWHERE countMatches(repeat(s, 1000000), 'a') > 0
SETTINGS max_execution_time = 0.5, timeout_overflow_mode = 'throw', max_block_size = 8, max_threads = 1
FORMAT Null; -- { serverError TIMEOUT_EXCEEDED }

SELECT 'ok';

DROP TABLE t_break_prewhere;
