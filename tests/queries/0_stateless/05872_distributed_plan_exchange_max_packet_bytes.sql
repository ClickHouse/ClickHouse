-- Tags: no-fasttest
-- no-fasttest: needs the streaming exchange of the stateless worker configuration.

-- `distributed_plan_exchange_max_packet_bytes` limits one packet of the streaming exchange. Chunks are
-- sent as packets of fewer rows; a row that does not fit fails the query, except a runtime filter
-- state, which is not sent: the join then runs without the filter.

DROP TABLE IF EXISTS t_packets_big;
DROP TABLE IF EXISTS t_packets_small;
-- Statistics with the number of distinct `bid` values: with the estimate that the planner makes without
-- them, as many distinct values as build keys, it would not send the filter.
CREATE TABLE t_packets_big (bid UInt64, v UInt64) ENGINE = MergeTree ORDER BY bid SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t_packets_small (sid UInt64) ENGINE = MergeTree ORDER BY sid;
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_packets_big;
SYSTEM STOP MERGES t_packets_small;
INSERT INTO t_packets_big SELECT number, number FROM numbers(100000);
-- Keys that do not compress, so that the state of the runtime filter is larger than one packet.
INSERT INTO t_packets_small SELECT sipHash64(number) % 100000 FROM numbers(10000);

SET make_distributed_plan = 1, enable_parallel_replicas = 0, max_rows_to_group_by = 0, enable_cascades_optimizer = 0;
SET distributed_plan_execute_locally = 0, distributed_plan_force_exchange_kind = 'Streaming';
SET distributed_plan_fallback_to_local_execution = 0;
SET distributed_plan_default_reader_bucket_count = 2, distributed_plan_default_shuffle_join_bucket_count = 2;
SET distributed_plan_max_rows_to_broadcast = 0, query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, distributed_plan_join_runtime_filters = 1;

SELECT '-- the expected result';
SELECT count(), sum(v) FROM t_packets_big, t_packets_small WHERE bid = sid SETTINGS make_distributed_plan = 0;

SELECT '-- packets of at most 4 KiB: the rows go in many packets, the filter is not sent';
SELECT count(), sum(v) FROM t_packets_big, t_packets_small WHERE bid = sid
SETTINGS distributed_plan_exchange_max_packet_bytes = 4096, log_comment = '05872_small_packets';

SELECT '-- a row larger than one packet';
SELECT length(groupArray(sipHash64(bid))) FROM t_packets_big
SETTINGS distributed_plan_exchange_max_packet_bytes = 65536; -- { serverError LIMIT_EXCEEDED }

SELECT '-- a limit too small for the end-of-stream packet';
SELECT count() FROM t_packets_big, t_packets_small WHERE bid = sid
SETTINGS distributed_plan_exchange_max_packet_bytes = 2; -- { serverError BAD_ARGUMENTS }

SET make_distributed_plan = 0;
SYSTEM FLUSH LOGS query_log;

SELECT '-- the filter state was not sent because of its size';
SELECT sum(tasks.ProfileEvents['RuntimeFilterOversizedStatesRejected']) > 0
FROM system.query_log AS tasks
INNER JOIN (
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND current_database = currentDatabase()
      AND log_comment = '05872_small_packets' AND type = 'QueryFinish') AS run
    ON tasks.initial_query_id = run.query_id
WHERE tasks.event_date >= yesterday() AND tasks.event_time >= now() - INTERVAL 10 MINUTE
  AND tasks.type = 'QueryFinish' AND tasks.query_id != run.query_id;

DROP TABLE t_packets_big;
DROP TABLE t_packets_small;
