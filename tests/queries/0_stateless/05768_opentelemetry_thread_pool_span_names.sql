-- The span of a thread pool job is named after the job: the name is passed to the pool at scheduling
-- (by the callback runner, by `ThreadFromGlobalPool`, or at a direct schedule call).
SET opentelemetry_start_trace_probability = 1;

-- The initiator merges the aggregated blocks of the shards on the aggregator's pool; the result is
-- pulled by the executor thread of the TCP handler.
SET log_comment = 'otel pool spans: query';
SELECT number % 200000 AS k, count() FROM remote('127.0.0.{1,2}', numbers_mt(500000)) GROUP BY k FORMAT Null
    SETTINGS max_threads = 4, group_by_two_level_threshold = 1000, distributed_aggregation_memory_efficient = 0, enable_parallel_replicas = 0;
SET log_comment = '';

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t VALUES (1);
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
-- The active parts are loaded on the parts loading pool.
SET log_comment = 'otel pool spans: attach';
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SET opentelemetry_start_trace_probability = 0, log_comment = '';
SYSTEM FLUSH LOGS query_log, opentelemetry_span_log;

CREATE TEMPORARY TABLE traced_queries ENGINE = Memory AS
SELECT DISTINCT query_id, replaceOne(log_comment, 'otel pool spans: ', '') AS label
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment LIKE 'otel pool spans: %';

CREATE TEMPORARY TABLE traces ENGINE = Memory AS
SELECT trace_id, label
FROM
(
    SELECT trace_id, attribute['clickhouse.query_id'] AS query_id
    FROM system.opentelemetry_span_log
    WHERE finish_date >= yesterday() AND operation_name = 'query'
        AND attribute['clickhouse.query_id'] IN (SELECT query_id FROM traced_queries)
) AS query_spans
JOIN traced_queries USING (query_id);

-- A pool span is logged when the worker finishes the job, which can be after the client has the result.
SELECT throwIf(countIf(label = 'query' AND operation_name = 'AggregatorPool') > 0 AND countIf(label = 'query' AND operation_name = 'QueryPullPipeEx') > 0
    AND countIf(label = 'attach' AND operation_name = 'ActiveParts') > 0)
FROM system.opentelemetry_span_log JOIN traces USING (trace_id)
WHERE finish_date >= yesterday() AND trace_id IN (SELECT trace_id FROM traces)
FORMAT Null; -- { retry 30 until serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SELECT DISTINCT label, operation_name
FROM system.opentelemetry_span_log JOIN traces USING (trace_id)
WHERE finish_date >= yesterday() AND trace_id IN (SELECT trace_id FROM traces)
    AND operation_name IN ('AggregatorPool', 'QueryPullPipeEx', 'ActiveParts')
ORDER BY ALL;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
