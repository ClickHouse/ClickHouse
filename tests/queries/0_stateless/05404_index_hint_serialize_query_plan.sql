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

-- The hinted column is also read, or the source is not MergeTree: the remote maps the column identifiers back.
SELECT sum(a), count() FROM remote('127.0.0.2', currentDatabase(), t_index_hint_serialized) WHERE indexHint(a < 10) SETTINGS serialize_query_plan = 0;
SELECT sum(a), count() FROM remote('127.0.0.2', currentDatabase(), t_index_hint_serialized) WHERE indexHint(a < 10) SETTINGS serialize_query_plan = 1;
SELECT sum(a), count() FROM remote('127.0.0.2', currentDatabase(), t_index_hint_serialized) PREWHERE indexHint(a < 10) SETTINGS serialize_query_plan = 0;
SELECT sum(a), count() FROM remote('127.0.0.2', currentDatabase(), t_index_hint_serialized) PREWHERE indexHint(a < 10) SETTINGS serialize_query_plan = 1;
SELECT count() FROM remote('127.0.0.2', numbers(100000)) WHERE indexHint(number < 10) SETTINGS serialize_query_plan = 0;
SELECT count() FROM remote('127.0.0.2', numbers(100000)) WHERE indexHint(number < 10) SETTINGS serialize_query_plan = 1;
-- A hint through a subquery alias does not prune on any path.
SELECT sum(x), count() FROM (SELECT a AS x FROM remote('127.0.0.2', currentDatabase(), t_index_hint_serialized)) WHERE indexHint(x < 10) SETTINGS serialize_query_plan = 0;
SELECT sum(x), count() FROM (SELECT a AS x FROM remote('127.0.0.2', currentDatabase(), t_index_hint_serialized)) WHERE indexHint(x < 10) SETTINGS serialize_query_plan = 1;

-- The CI profile sets max_rows_to_group_by, which a distributed aggregation rejects.
SELECT sum(b), count() FROM t_index_hint_serialized WHERE indexHint(a < 10) SETTINGS make_distributed_plan = 1, distributed_plan_fallback_to_local_execution = 0, max_rows_to_group_by = 0;
SELECT sum(b), count() FROM t_index_hint_serialized WHERE indexHint(a IN (SELECT number FROM numbers(10))) SETTINGS make_distributed_plan = 1, distributed_plan_fallback_to_local_execution = 0, max_rows_to_group_by = 0;
SELECT sum(a), count() FROM t_index_hint_serialized WHERE indexHint(a < 10) SETTINGS make_distributed_plan = 1, distributed_plan_fallback_to_local_execution = 0, max_rows_to_group_by = 0;

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

-- Plan-based parallel replicas ship the sets of `indexHint` with the fragment, so an unshippable one keeps the read local.
-- `generateRandom` cannot be serialized; with `max_string_length = 0` it returns only empty strings.
SET enable_parallel_replicas = 1, parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_plan_based = 1, automatic_parallel_replicas_mode = 0;
SELECT sum(b), count() FROM t_index_hint_serialized WHERE indexHint(a IN (SELECT length(x) FROM generateRandom('x String', 1, 0, 1) LIMIT 3));
SELECT sum(b), count() FROM t_index_hint_serialized WHERE indexHint(b IN (SELECT length(x) FROM generateRandom('x String', 1, 0, 1) LIMIT 3));
SELECT sum(b), count() FROM t_index_hint_serialized WHERE arrayExists(y -> indexHint(y IN (SELECT length(x) FROM generateRandom('x String', 1, 0, 1) LIMIT 3)), [a]);
SELECT sum(b), count() FROM t_index_hint_serialized WHERE b IN (SELECT a FROM t_index_hint_serialized WHERE indexHint(a IN (SELECT length(x) FROM generateRandom('x String', 1, 0, 1) LIMIT 3)));
SELECT sum(b), count() FROM t_index_hint_serialized WHERE b IN (SELECT number FROM numbers(10) LIMIT 3 AFTER indexHint(number IN (SELECT length(x) FROM generateRandom('x String', 1, 0, 1) LIMIT 3)));
SELECT sum(b), count() FROM t_index_hint_serialized WHERE b IN (SELECT n FROM (SELECT number AS n, toUInt8(1) AS v FROM numbers(10)) ORDER BY n WITH FILL TO 12
    INTERPOLATE (v AS indexHint(v IN (SELECT length(x) FROM generateRandom('x String', 1, 0, 1) LIMIT 3))));
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') = 0 FROM (EXPLAIN SELECT sum(b) FROM t_index_hint_serialized WHERE indexHint(a IN (SELECT length(x) FROM generateRandom('x String', 1, 0, 1) LIMIT 3)));
SELECT sum(b), count() FROM t_index_hint_serialized WHERE indexHint(a IN (SELECT number FROM numbers(10)));
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 FROM (EXPLAIN SELECT sum(b) FROM t_index_hint_serialized WHERE indexHint(a IN (SELECT number FROM numbers(10))));

-- A shipped read maps the column identifiers back, including `numbers` on the broadcast side of a shipped join.
-- A randomized join order can keep the join local.
SELECT sum(a), count() FROM t_index_hint_serialized WHERE indexHint(a < 10) SETTINGS parallel_replicas_local_plan = 0;
SELECT sum(a), count() FROM t_index_hint_serialized WHERE indexHint(a < 10) SETTINGS parallel_replicas_local_plan = 1;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 FROM (EXPLAIN SELECT sum(a) FROM t_index_hint_serialized WHERE indexHint(a < 10) SETTINGS parallel_replicas_local_plan = 0);
SELECT count(), sum(r.a) FROM t_index_hint_serialized AS r INNER JOIN numbers(100000) AS l ON l.number = r.a WHERE indexHint(l.number < 3)
    SETTINGS parallel_replicas_local_plan = 0, query_plan_optimize_join_order_randomize = 0;
SELECT countIf(explain ILIKE '%JoinLogical%') > 0 FROM (EXPLAIN SELECT count(), sum(r.a) FROM t_index_hint_serialized AS r INNER JOIN numbers(100000) AS l ON l.number = r.a WHERE indexHint(l.number < 3)
    SETTINGS parallel_replicas_local_plan = 0, query_plan_optimize_join_order_randomize = 0);

-- A hint inside a lambda in a filter fused into a shipped `ARRAY JOIN` (fusion is skipped for serialized plans).
SELECT count(), sum(l.b) FROM t_index_hint_serialized AS l
    INNER JOIN (SELECT number AS k FROM numbers(20) ARRAY JOIN [number] AS elem WHERE arrayExists(y -> y = elem AND indexHint(y < 10), [elem])) AS r ON l.a = r.k
    SETTINGS serialize_query_plan = 0, query_plan_join_swap_table = false, query_plan_optimize_join_order_randomize = 0;
SELECT countIf(explain LIKE '%ArrayJoin%') = 2 AND countIf(explain LIKE '%Element filter%') = 1 AND countIf(explain LIKE '%Filter (WHERE)%') = 0
FROM (EXPLAIN actions = 1 SELECT count(), sum(l.b) FROM t_index_hint_serialized AS l
    INNER JOIN (SELECT number AS k FROM numbers(20) ARRAY JOIN [number] AS elem WHERE arrayExists(y -> y = elem AND indexHint(y < 10), [elem])) AS r ON l.a = r.k
    SETTINGS serialize_query_plan = 0, parallel_replicas_local_plan = 1, query_plan_max_step_description_length = 1000000,
        query_plan_join_swap_table = false, query_plan_optimize_join_order_randomize = 0);

DROP TABLE t_index_hint_serialized;
