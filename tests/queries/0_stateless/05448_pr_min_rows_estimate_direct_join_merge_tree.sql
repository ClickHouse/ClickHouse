-- A positive `parallel_replicas_min_number_of_rows_per_replica` makes the planner collect the pushed-down filters
-- for `MergeTree` tables by planning the query once more over dummy tables. With `join_algorithm = 'direct'`
-- that dummy plan tried to build a direct join with a `MergeTree` right table, whose read had been replaced
-- by a dummy one, and threw `NOT_IMPLEMENTED`, while the same query without the setting worked.

DROP TABLE IF EXISTS dj_l;
DROP TABLE IF EXISTS dj_r;

CREATE TABLE dj_l (Id UInt64) ENGINE = MergeTree ORDER BY Id;
CREATE TABLE dj_r (EventId UInt64, Attribute String) ENGINE = MergeTree ORDER BY EventId;
INSERT INTO dj_l SELECT number FROM numbers(1000);
INSERT INTO dj_r SELECT number, toString(number) FROM numbers(1000);

SET join_algorithm = 'direct';
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_min_number_of_rows_per_replica = 1;

SELECT count() FROM dj_l AS t0 INNER JOIN dj_r AS t1 ON t1.EventId = t0.Id;

-- The join in a subquery is planned with the subquery's own context.
SELECT count() FROM (SELECT t1.Attribute AS a FROM dj_l AS t0 INNER JOIN dj_r AS t1 ON t1.EventId = t0.Id WHERE t0.Id > 10) WHERE a != '';

SELECT count() FROM (SELECT t0.Id FROM dj_l AS t0 LEFT SEMI JOIN dj_r AS t1 ON t1.EventId = t0.Id) WHERE Id < 500;
SELECT count() FROM dj_l AS t0 LEFT ANTI JOIN dj_r AS t1 ON t1.EventId = t0.Id;

DROP TABLE dj_l;
DROP TABLE dj_r;
