-- Tags: no-fasttest
-- no-fasttest: needs the streaming exchange of the stateless worker configuration.

-- A task of a distributed query plan combines small chunks before it sends them.

DROP TABLE IF EXISTS t_exchange_small_chunks;
CREATE TABLE t_exchange_small_chunks (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO t_exchange_small_chunks SELECT number, number FROM numbers(400000);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0;
SET distributed_plan_default_reader_bucket_count = 2, distributed_plan_default_shuffle_join_bucket_count = 2;
-- Shuffle rows, not aggregation states: states are not combined.
SET distributed_plan_force_shuffle_aggregation = 1;

-- A read step takes 1024 rows and 1 of 100 passes the filter: about 400 chunks of 10 rows.
-- `max_threads` fixes the number of streams.
SELECT count(), sum(c) FROM (SELECT k, count() AS c FROM t_exchange_small_chunks WHERE v % 100 = 0 GROUP BY k)
  SETTINGS max_block_size = 1024, max_threads = 2, log_comment = '05830_small_chunks';

SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log;

-- With one packet for each chunk, the shuffle to two destinations would send about 800 packets.
SELECT sum(tasks.ProfileEvents['StreamingExchangePacketsSent']) BETWEEN 1 AND 199 AS few_packets
FROM system.query_log AS tasks
INNER JOIN (
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND current_database = currentDatabase()
      AND log_comment = '05830_small_chunks' AND type = 'QueryFinish') AS run
    ON tasks.initial_query_id = run.query_id
WHERE tasks.event_date >= yesterday() AND tasks.event_time >= now() - INTERVAL 10 MINUTE
  AND tasks.type = 'QueryFinish' AND tasks.query_id != run.query_id;

DROP TABLE t_exchange_small_chunks;
