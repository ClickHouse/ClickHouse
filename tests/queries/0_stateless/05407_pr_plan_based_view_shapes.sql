-- A `SETTINGS` clause in a view body opts the inner read out of parallel replicas. Plan-based honours
-- it because it reads the setting from the read step's own context, which is the view's; a setting
-- given on the query would be the same in either context and so cannot tell that apart.
-- `04616_pr_plan_based_view_non_replicated_optin` covers the query-level form.

DROP VIEW IF EXISTS v_optout;
DROP VIEW IF EXISTS v_plain;
DROP TABLE IF EXISTS t_view_optout;

CREATE TABLE t_view_optout (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_view_optout SELECT number FROM numbers(1000);

CREATE VIEW v_plain AS SELECT a FROM t_view_optout;
CREATE VIEW v_optout AS SELECT a FROM t_view_optout SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0;

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_plan_based = 1;

-- The view without the opt-out is distributed.
SELECT sum(a) FROM v_plain;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS distributed
FROM (EXPLAIN optimize = 1, description = 0 SELECT sum(a) FROM v_plain);

-- The one whose body opts out is not, and still answers for the whole table rather than once per
-- replica.
SELECT sum(a) FROM v_optout;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS distributed
FROM (EXPLAIN optimize = 1, description = 0 SELECT sum(a) FROM v_optout);

DROP VIEW v_optout;
DROP VIEW v_plain;
DROP TABLE t_view_optout;
