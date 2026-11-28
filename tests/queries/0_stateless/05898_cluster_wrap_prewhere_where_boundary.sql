-- Tags: no-fasttest
-- no-fasttest: `fileCluster` is not in the fast test build.
-- A `PREWHERE` moved onto the wrap of an `IStorageCluster` table in a JOIN becomes the wrap `WHERE`.
-- It must not be merged there with the copied outer `WHERE`: the outer `WHERE` may throw on rows
-- that the `PREWHERE` excludes, and one `and` does not keep the evaluation order between them
-- when short-circuit evaluation is disabled.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET optimize_move_to_prewhere = 0;
SET query_plan_optimize_prewhere = 0;
SET short_circuit_function_evaluation = 'disable';

INSERT INTO FUNCTION file(currentDatabase() || '_05898_wrap_left.tsv', 'TSV', 'n UInt64')
SELECT number
FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_05898_right;
CREATE TABLE t_05898_right
(
    n UInt64
)
ENGINE = Memory;
-- No `0` here: filter push-down also derives `intDiv(1, r.n) > 0` for the right table from `l.n = r.n`,
-- which is not what this test is about.
INSERT INTO t_05898_right VALUES (1), (2);

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05898_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
LEFT JOIN t_05898_right AS r ON l.n = r.n
PREWHERE l.n != 0
WHERE intDiv(1, l.n) > 0;

SELECT count()
FROM
(
    SELECT *
    FROM fileCluster(
        'test_cluster_one_shard_two_replicas',
        currentDatabase() || '_05898_wrap_left.tsv',
        'TSV',
        'n UInt64') AS l
    LEFT JOIN t_05898_right AS r ON l.n = r.n
    PREWHERE l.n != 0
    WHERE intDiv(1, l.n) > 0
);

-- The outer `WHERE` is still applied, on the initiator.
SELECT l.n
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05898_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
LEFT JOIN t_05898_right AS r ON l.n = r.n
PREWHERE l.n != 0
WHERE l.n < 2
ORDER BY l.n;

DROP TABLE t_05898_right;
