-- Tags: no-fasttest
-- no-fasttest: `fileCluster` is not in the fast test build.
-- A stateful function left above the JOIN must see the unfiltered stream. Copying the stateless
-- `l.n = 1` conjunct into the `IStorageCluster` wrap would drop the preceding row, so
-- `rowNumberInAllBlocks` would assign zero to `n = 1` and the query would return 1 instead of 0.

SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET allow_push_predicate_ast_for_distributed_subqueries = 0;
SET short_circuit_function_evaluation = 'disable';
SET join_algorithm = 'hash';
SET max_threads = 1;
SET max_block_size = 100;

INSERT INTO FUNCTION file(currentDatabase() || '_05458_wrap_left.tsv', 'TSV', 'n UInt64')
SELECT number
FROM numbers(3)
SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_wrap_right;
CREATE TABLE t_wrap_right
(
    n UInt64
)
ENGINE = Memory;
INSERT INTO t_wrap_right VALUES (0), (1), (2);

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05458_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
INNER JOIN t_wrap_right AS r ON l.n = r.n
WHERE rowNumberInAllBlocks() = 0 AND l.n = 1;

SELECT count()
FROM (SELECT * FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05458_wrap_left.tsv',
    'TSV',
    'n UInt64')) AS l
INNER JOIN t_wrap_right AS r ON l.n = r.n
WHERE rowNumberInAllBlocks() = 0 AND l.n = 1;

DROP TABLE t_wrap_right;
