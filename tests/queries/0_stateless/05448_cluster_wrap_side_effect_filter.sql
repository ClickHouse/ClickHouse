-- Tags: no-fasttest
-- no-fasttest: `fileCluster` is not in the fast test build.
-- `sleepEachRow` is deterministic and stateless, but it has observable side effects.
-- Copying it into the `IStorageCluster` wrap would evaluate it on every source row
-- instead of only on the JOIN result, and exceed `function_sleep_max_microseconds_per_block`.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_05448_wrap_left.tsv', 'TSV', 'n UInt64')
SELECT number
FROM numbers(1000)
SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_wrap_right;
CREATE TABLE t_wrap_right
(
    n UInt64
)
ENGINE = Memory;
INSERT INTO t_wrap_right VALUES (0);

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05448_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
INNER JOIN t_wrap_right AS r ON l.n = r.n
WHERE sleepEachRow(0.001) = 0
SETTINGS function_sleep_max_microseconds_per_block = 10000;

DROP TABLE t_wrap_right;
