-- Tags: no-fasttest
-- Tag no-fasttest: IcebergLocal needs Parquet

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

-- A cluster table function sends its filter to the replicas without `indexHint`.
CREATE TABLE t_index_hint_iceberg (a UInt32) ENGINE = IcebergLocal(currentDatabase() || '_05404/', 'Parquet');
INSERT INTO t_index_hint_iceberg SETTINGS allow_insert_into_iceberg = 1 SELECT number FROM numbers(1000);
SELECT count() FROM icebergLocalCluster('test_cluster_two_shards_localhost', currentDatabase() || '_05404/', 'Parquet') AS t, (SELECT number AS k FROM numbers(1000)) AS n
    WHERE t.a = n.k AND t.a % 2 = 0 AND indexHint(t.a IN (SELECT number FROM numbers(10)));
SYSTEM FLUSH LOGS query_log;
-- The replica queries run in `default`, so they are matched through the initiator's query_id.
SELECT count() > 0, countIf(Settings['cluster_table_function_pushed_down_filter'] != '') = count(), sum(result_rows) < 1000 FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND NOT is_initial_query
        AND initial_query_id IN (
            SELECT query_id FROM system.query_log
            WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query
                AND query LIKE '%icebergLocalCluster%');
DROP TABLE t_index_hint_iceberg;

DROP TABLE t_index_hint_serialized;
