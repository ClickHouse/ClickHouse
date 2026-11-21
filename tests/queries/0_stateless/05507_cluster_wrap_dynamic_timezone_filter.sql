-- Tags: no-fasttest
-- no-fasttest: `fileCluster` is not in the fast test build.
-- `Dynamic` and `JSON` hide the runtime types of their values, so a value can be a `DateTime`
-- without an explicit time zone, which a cluster worker with a different default time zone renders
-- differently (`concat(l.v, '')`). A conjunct with such an argument must stay on the initiator
-- instead of being copied into the `WHERE` of the `IStorageCluster` JOIN wrap sent to the workers.
-- A string function over a `String` column is still copied.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_05507_wrap_left.native', 'Native', 'n UInt64, s String, v Dynamic, j JSON')
SELECT number, 'x', toDateTime(number, 'UTC')::Dynamic, '{"a" : "1970-01-01 00:00:00"}'::JSON
FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_05507_right;
CREATE TABLE t_05507_right
(
    n UInt64
)
ENGINE = Memory;
INSERT INTO t_05507_right VALUES (0), (1), (2);

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05507_wrap_left.native',
    'Native',
    'n UInt64, s String, v Dynamic, j JSON') AS l
INNER JOIN t_05507_right AS r ON l.n = r.n
WHERE concat(l.v, '') != '' AND l.n < 2
SETTINGS log_comment = '05507_q_dynamic';

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05507_wrap_left.native',
    'Native',
    'n UInt64, s String, v Dynamic, j JSON') AS l
INNER JOIN t_05507_right AS r ON l.n = r.n
WHERE concat(l.j.a, '') != '' AND l.n < 2
SETTINGS log_comment = '05507_q_json_path';

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05507_wrap_left.native',
    'Native',
    'n UInt64, s String, v Dynamic, j JSON') AS l
INNER JOIN t_05507_right AS r ON l.n = r.n
WHERE concat(l.s, '') != '' AND l.n < 2
SETTINGS log_comment = '05507_q_string';

SYSTEM FLUSH LOGS query_log;

-- The workers received queries; only the one over a `String` column refers to `concat`.
SELECT log_comment, count() > 0, countIf(match(query, 'concat')) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time > now() - 600
    AND type = 'QueryFinish' AND is_initial_query = 0
    AND startsWith(log_comment, '05507_q_')
    AND initial_query_id IN (
        SELECT query_id
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time > now() - 600
            AND current_database = currentDatabase() AND is_initial_query = 1
            AND startsWith(log_comment, '05507_q_'))
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_05507_right;
