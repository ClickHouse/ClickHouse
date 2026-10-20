DROP TABLE IF EXISTS t_index_hint_serialized;
CREATE TABLE t_index_hint_serialized (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 128, index_granularity_bytes = '10Mi';
INSERT INTO t_index_hint_serialized SELECT intDiv(number, 100), number % 997 FROM numbers(100000);
OPTIMIZE TABLE t_index_hint_serialized FINAL;

-- A remote server rebuilds `indexHint` from the serialized plan; it must keep the predicate.
SET prefer_localhost_replica = 0, use_query_condition_cache = 0;

SELECT sum(b), count() FROM remote('127.0.0.2', currentDatabase(), t_index_hint_serialized) WHERE indexHint(a < 10) SETTINGS serialize_query_plan = 0;
SELECT sum(b), count() FROM remote('127.0.0.2', currentDatabase(), t_index_hint_serialized) WHERE indexHint(a < 10) SETTINGS serialize_query_plan = 1;

SELECT sum(b), count() FROM remote('127.0.0.2', currentDatabase(), t_index_hint_serialized) PREWHERE indexHint(a < 10) SETTINGS serialize_query_plan = 0;
SELECT sum(b), count() FROM remote('127.0.0.2', currentDatabase(), t_index_hint_serialized) PREWHERE indexHint(a < 10) SETTINGS serialize_query_plan = 1;

SELECT sum(b), count() FROM remote('127.0.0.2', currentDatabase(), t_index_hint_serialized) WHERE b > 500 AND indexHint(a < 10 AND indexHint(a > 2)) SETTINGS serialize_query_plan = 0;
SELECT sum(b), count() FROM remote('127.0.0.2', currentDatabase(), t_index_hint_serialized) WHERE b > 500 AND indexHint(a < 10 AND indexHint(a > 2)) SETTINGS serialize_query_plan = 1;

SELECT sum(b), count() FROM remote('127.0.0.2', currentDatabase(), t_index_hint_serialized) WHERE indexHint(a IN (SELECT number FROM numbers(10))) SETTINGS serialize_query_plan = 0;
SELECT sum(b), count() FROM remote('127.0.0.2', currentDatabase(), t_index_hint_serialized) WHERE indexHint(a IN (SELECT number FROM numbers(10))) SETTINGS serialize_query_plan = 1;

-- The CI profile sets max_rows_to_group_by, which a distributed aggregation rejects.
SELECT sum(b), count() FROM t_index_hint_serialized WHERE indexHint(a < 10) SETTINGS make_distributed_plan = 1, distributed_plan_fallback_to_local_execution = 0, max_rows_to_group_by = 0;
SELECT sum(b), count() FROM t_index_hint_serialized WHERE indexHint(a IN (SELECT number FROM numbers(10))) SETTINGS make_distributed_plan = 1, distributed_plan_fallback_to_local_execution = 0, max_rows_to_group_by = 0;

DROP TABLE t_index_hint_serialized;
