-- Tags: no-fasttest
-- no-fasttest: `fileCluster` is not in the fast test build.
-- A `DateTime` without an explicit time zone is interpreted in the default time zone of the
-- server that evaluates it, and a cluster worker can have a different one than the initiator.
-- A conjunct with such a value (`toHour(toDateTime(l.n))`) must stay on the initiator instead
-- of being copied into the `WHERE` of the `IStorageCluster` JOIN wrap sent to the workers.
-- The same holds for functions whose types contain no `DateTime` but still use the default
-- time zone: `toUnixTimestamp(String)`, `toDate(UInt64)`, `toRelativeSecondNum(Date)` and
-- `fromUnixTimestamp(UInt64, String)`. Date and time functions and type conversions are never
-- copied, even with an explicit time zone. A string function (`lower`) is still copied.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_05480_wrap_left.tsv', 'TSV', 'n UInt64, s String, ts UInt64, d Date')
SELECT number, '2024-01-01 00:00:00', 1704124800, '2024-01-01'
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
    'n UInt64, s String, ts UInt64, d Date') AS l
INNER JOIN t_05480_right AS r ON l.n = r.n
WHERE toHour(toDateTime(l.n)) = toHour(toDateTime(0)) AND l.n < 2
SETTINGS log_comment = '05480_q_implicit';

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05480_wrap_left.tsv',
    'TSV',
    'n UInt64, s String, ts UInt64, d Date') AS l
INNER JOIN t_05480_right AS r ON l.n = r.n
WHERE toHour(toDateTime(l.n, 'UTC')) = 0 AND l.n < 2
SETTINGS log_comment = '05480_q_explicit';

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05480_wrap_left.tsv',
    'TSV',
    'n UInt64, s String, ts UInt64, d Date') AS l
INNER JOIN t_05480_right AS r ON l.n = r.n
WHERE toUnixTimestamp(l.s) = toUnixTimestamp('2024-01-01 00:00:00') AND l.n < 2
SETTINGS log_comment = '05480_q_unix_timestamp_implicit';

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05480_wrap_left.tsv',
    'TSV',
    'n UInt64, s String, ts UInt64, d Date') AS l
INNER JOIN t_05480_right AS r ON l.n = r.n
WHERE toUnixTimestamp(l.s, 'UTC') = 1704067200 AND l.n < 2
SETTINGS log_comment = '05480_q_unix_timestamp_explicit';

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05480_wrap_left.tsv',
    'TSV',
    'n UInt64, s String, ts UInt64, d Date') AS l
INNER JOIN t_05480_right AS r ON l.n = r.n
WHERE toDate(l.ts) = toDate(toUInt64(1704124800)) AND l.n < 2
SETTINGS log_comment = '05480_q_to_date_implicit';

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05480_wrap_left.tsv',
    'TSV',
    'n UInt64, s String, ts UInt64, d Date') AS l
INNER JOIN t_05480_right AS r ON l.n = r.n
WHERE toRelativeSecondNum(l.d) = toRelativeSecondNum(toDate('2024-01-01')) AND l.n < 2
SETTINGS log_comment = '05480_q_relative_second_num_implicit';

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05480_wrap_left.tsv',
    'TSV',
    'n UInt64, s String, ts UInt64, d Date') AS l
INNER JOIN t_05480_right AS r ON l.n = r.n
WHERE fromUnixTimestamp(l.n, '%H') = fromUnixTimestamp(0, '%H') AND l.n < 2
SETTINGS log_comment = '05480_q_from_unix_timestamp_implicit';

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05480_wrap_left.tsv',
    'TSV',
    'n UInt64, s String, ts UInt64, d Date') AS l
INNER JOIN t_05480_right AS r ON l.n = r.n
WHERE lower(l.s) = '2024-01-01 00:00:00' AND l.n < 2
SETTINGS log_comment = '05480_q_string_function';

SYSTEM FLUSH LOGS query_log;

-- The workers received queries; only the one with a string function refers to the function.
SELECT log_comment, count() > 0,
    countIf(match(query, 'toHour|toUnixTimestamp|toDate|toRelativeSecondNum|fromUnixTimestamp|lower')) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time > now() - 600
    AND type = 'QueryFinish' AND is_initial_query = 0
    AND startsWith(log_comment, '05480_q_')
    AND initial_query_id IN (
        SELECT query_id
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time > now() - 600
            AND current_database = currentDatabase() AND is_initial_query = 1
            AND startsWith(log_comment, '05480_q_'))
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_05480_right;
