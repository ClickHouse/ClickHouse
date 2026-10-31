-- When the destination of a `Buffer` table is `Distributed`, the buffers are read with `StorageValues`. The read limits of the
-- query apply to the rows in the buffers, as to any storage.

DROP TABLE IF EXISTS t_buffer;
DROP TABLE IF EXISTS t_buffer_distributed;
DROP TABLE IF EXISTS t_buffer_local;

CREATE TABLE t_buffer_local (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_buffer_distributed (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_buffer_local);
-- The thresholds are never reached, so the rows stay in the buffer.
CREATE TABLE t_buffer (x UInt64) ENGINE = Buffer(currentDatabase(), t_buffer_distributed, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO t_buffer SELECT number FROM numbers(100);

SELECT countIf(explain ILIKE '%ReadFromStorage (Values)%') FROM (EXPLAIN SELECT sum(x) FROM t_buffer) SETTINGS explain_query_plan_default = 'legacy';
SELECT sum(x) FROM t_buffer;
SELECT sum(x) FROM t_buffer SETTINGS max_rows_to_read = 10; -- { serverError TOO_MANY_ROWS }

DROP TABLE t_buffer;
DROP TABLE t_buffer_distributed;
DROP TABLE t_buffer_local;
