-- Tags: no-fasttest
-- no-fasttest: needs the streaming exchange of the stateless worker configuration.

-- A receiver of a streaming exchange parses each column type once and takes it from its own map for the
-- later columns and packets. The packets of the query below have two key columns of the same type, so each
-- packet with columns reuses at least one parsed type. The result must stay the same as without the distributed plan.

DROP TABLE IF EXISTS t_exchange_types;
CREATE TABLE t_exchange_types (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_exchange_types SELECT number FROM numbers(20000);

SET enable_parallel_replicas = 0, max_rows_to_group_by = 0;
SET distributed_plan_default_reader_bucket_count = 1, distributed_plan_default_shuffle_join_bucket_count = 3;

SELECT count(), sum(c), sum(s), sum(u) FROM
(
    SELECT toUInt64(k % 1000) AS g, toUInt64(k % 7) AS h, count() AS c, sum(k) AS s, uniq(k) AS u FROM t_exchange_types GROUP BY g, h
)
SETTINGS make_distributed_plan = 1, distributed_plan_force_shuffle_aggregation = 1,
    log_comment = '05565_distributed_plan_exchange_reuses_parsed_types';

SELECT count(), sum(c), sum(s), sum(u) FROM
(
    SELECT toUInt64(k % 1000) AS g, toUInt64(k % 7) AS h, count() AS c, sum(k) AS s, uniq(k) AS u FROM t_exchange_types GROUP BY g, h
)
SETTINGS make_distributed_plan = 0;

SYSTEM FLUSH LOGS query_log;

-- The tasks of the query have its id as `initial_query_id`. On old code the event does not exist, and the sum is 0.
SELECT sum(ProfileEvents['NativeReaderTypeCacheHits']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND type = 'QueryFinish'
  AND initial_query_id IN (
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE AND current_database = currentDatabase()
      AND log_comment = '05565_distributed_plan_exchange_reuses_parsed_types' AND type = 'QueryFinish');

DROP TABLE t_exchange_types;
