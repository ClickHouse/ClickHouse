-- A spilled `DISTINCT` or `LIMIT BY` returns its remaining rows only when its input ends, which an unbounded
-- `STREAM` never does, so a streaming query keeps both in memory. A 1-byte threshold would make them spill
-- from their first chunk, and a spilled query would return no rows before `max_execution_time` breaks it.
SET enable_streaming_queries = 1;
SET enable_parallel_replicas = 0;
SET max_bytes_ratio_before_external_distinct = 0, max_bytes_before_external_distinct = 1;
SET max_bytes_ratio_before_external_limit_by = 0, max_bytes_before_external_limit_by = 1;
SET max_untracked_memory = 0;

DROP TABLE IF EXISTS t_stream_group_limit;
CREATE TABLE t_stream_group_limit (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_stream_group_limit SELECT number % 100 FROM numbers(1000);

SELECT k FROM t_stream_group_limit STREAM LIMIT 1 BY k LIMIT 3
SETTINGS max_threads = 1, max_execution_time = 20, timeout_overflow_mode = 'break';
SELECT DISTINCT k FROM t_stream_group_limit STREAM LIMIT 3
SETTINGS max_threads = 1, max_execution_time = 20, timeout_overflow_mode = 'break';

DROP TABLE t_stream_group_limit;
