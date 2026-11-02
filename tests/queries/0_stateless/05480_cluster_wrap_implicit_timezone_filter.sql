-- Tags: no-fasttest
-- no-fasttest: `fileCluster` is not in the fast test build.
-- A `DateTime` without an explicit time zone is interpreted in the default time zone of the
-- server that evaluates it, and a cluster worker can have a different one than the initiator.
-- A conjunct with such a value (`toHour(toDateTime(l.n))`) must stay on the initiator instead
-- of being copied into the `WHERE` of the `IStorageCluster` JOIN wrap sent to the workers.
-- The same conjunct with an explicit time zone is still copied.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_05480_wrap_left.tsv', 'TSV', 'n UInt64')
SELECT number
FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_05480_right;
CREATE TABLE t_05480_right
(
    n UInt64
)
ENGINE = Memory;
INSERT INTO t_05480_right VALUES (0), (1), (2);

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05480_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
INNER JOIN t_05480_right AS r ON l.n = r.n
WHERE toHour(toDateTime(l.n)) = toHour(toDateTime(0)) AND l.n < 2
SETTINGS log_comment = '05480_implicit';

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05480_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
INNER JOIN t_05480_right AS r ON l.n = r.n
WHERE toHour(toDateTime(l.n, 'UTC')) = 0 AND l.n < 2
SETTINGS log_comment = '05480_explicit';

SYSTEM FLUSH LOGS query_log;

-- The workers received queries; only the one with the explicit time zone refers to `toHour`.
SELECT log_comment, count() > 0, countIf(position(query, 'toHour') > 0) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time > now() - 600
    AND type = 'QueryFinish' AND is_initial_query = 0
    AND log_comment IN ('05480_implicit', '05480_explicit')
    AND initial_query_id IN (
        SELECT query_id
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time > now() - 600
            AND current_database = currentDatabase() AND is_initial_query = 1
            AND log_comment IN ('05480_implicit', '05480_explicit'))
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_05480_right;
