-- Tags: no-fasttest
-- no-fasttest: `fileCluster` is not in the fast test build.
-- Copying left-side `WHERE` conjuncts into the cluster wrap is a form of filter push-down,
-- so it must be disabled together with it. Here `r.flag` guards `intDiv(1, l.n)` under
-- short-circuit evaluation; copying only `intDiv(1, l.n) > 0` would throw on `l.n = 0`.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET allow_push_predicate_ast_for_distributed_subqueries = 0;
SET short_circuit_function_evaluation = 'force_enable';
SET short_circuit_function_evaluation_reorder_arguments = 0;

INSERT INTO FUNCTION file(currentDatabase() || '_05457_wrap_left.tsv', 'TSV', 'n UInt64')
SELECT number
FROM numbers(2)
SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS t_05457_right;
CREATE TABLE t_05457_right
(
    n UInt64,
    flag UInt8
)
ENGINE = Memory;
INSERT INTO t_05457_right VALUES (0, 0), (1, 1);

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05457_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
INNER JOIN t_05457_right AS r ON l.n = r.n
WHERE r.flag AND intDiv(1, l.n) > 0
SETTINGS query_plan_filter_push_down = 0;

SELECT count()
FROM fileCluster(
    'test_cluster_one_shard_two_replicas',
    currentDatabase() || '_05457_wrap_left.tsv',
    'TSV',
    'n UInt64') AS l
INNER JOIN t_05457_right AS r ON l.n = r.n
WHERE r.flag AND intDiv(1, l.n) > 0
SETTINGS query_plan_enable_optimizations = 0;

DROP TABLE t_05457_right;
