-- A client that waits for each async insert sends the next one only after the previous one is flushed, so its inserts
-- are never batched. After its first insert waits the busy timeout alone, the next ones must be flushed at once.

DROP TABLE IF EXISTS t_async_sequential;
CREATE TABLE t_async_sequential (n UInt64) ENGINE = MergeTree ORDER BY n;

SET async_insert = 1, wait_for_async_insert = 1, async_insert_use_adaptive_busy_timeout = 1,
    async_insert_busy_timeout_min_ms = 3000, async_insert_busy_timeout_max_ms = 10000;

INSERT INTO t_async_sequential VALUES (1);
INSERT INTO t_async_sequential VALUES (2);
INSERT INTO t_async_sequential VALUES (3);
INSERT INTO t_async_sequential VALUES (4);
INSERT INTO t_async_sequential VALUES (5);
INSERT INTO t_async_sequential VALUES (6);
INSERT INTO t_async_sequential VALUES (7);
INSERT INTO t_async_sequential VALUES (8);
INSERT INTO t_async_sequential VALUES (9);
INSERT INTO t_async_sequential VALUES (10);

SELECT count() FROM t_async_sequential;

SYSTEM FLUSH LOGS query_log;

-- Inserts of concurrent tests can make the queue shard busy and delay some of the inserts, so require only one fast one.
SELECT countIf(query_duration_ms < 3000) >= 1
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
    AND query_kind = 'Insert' AND query LIKE 'INSERT INTO t_async_sequential VALUES%';

DROP TABLE t_async_sequential;
