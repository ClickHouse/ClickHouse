-- Tags: no-fasttest
-- no-fasttest: needs the streaming exchange of the stateless worker configuration.

-- A task of a distributed query plan sends a chunk larger than one exchange packet as several packets,
-- each with a range of its rows. Here the reading task sends one chunk with three `groupArray` states
-- of about 16 MB each, of values that do not compress.

DROP TABLE IF EXISTS t_exchange_large_states;
CREATE TABLE t_exchange_large_states (v UInt64 CODEC(Delta, LZ4)) ENGINE = MergeTree ORDER BY tuple();
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_exchange_large_states;
INSERT INTO t_exchange_large_states SELECT number FROM numbers(6000000);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0, enable_cascades_optimizer = 0;
SET distributed_plan_execute_locally = 0, distributed_plan_force_exchange_kind = 'Streaming';
SET distributed_plan_fallback_to_local_execution = 0;
-- One reading task and one merging task: the reading task sends all its states to one destination.
SET distributed_plan_default_reader_bucket_count = 1, distributed_plan_default_shuffle_join_bucket_count = 1;
-- The reading task makes one chunk with the three states: no two-level or external aggregation, and
-- blocks of more than three rows.
SET group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0, max_block_size = 65409;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT v % 3 AS g, count(), sum(v), arraySum(groupArray(sipHash64(v)))
FROM t_exchange_large_states GROUP BY g ORDER BY g
SETTINGS log_comment = '05871_large_states';

SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log;

-- With one packet for each chunk, the reading task would send one data packet and the end-of-stream marker.
SELECT countIf(tasks.ProfileEvents['StreamingExchangePacketsSent'] >= 4) = 1 AS split
FROM system.query_log AS tasks
INNER JOIN (
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND current_database = currentDatabase()
      AND log_comment = '05871_large_states' AND type = 'QueryFinish') AS run
    ON tasks.initial_query_id = run.query_id
WHERE tasks.event_date >= yesterday() AND tasks.event_time >= now() - INTERVAL 10 MINUTE
  AND tasks.type = 'QueryFinish' AND tasks.query_id LIKE '%::stage_0_%';

DROP TABLE t_exchange_large_states;
