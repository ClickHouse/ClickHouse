-- An explicit `PREWHERE` over a `Distributed` table keeps the classic read: the plan-level read of
-- distributed lazy materialization would select the Top-K candidates over the unfiltered rows.

SET serialize_query_plan = 1;
SET query_plan_optimize_distributed_lazy_materialization = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET distributed_push_down_limit = 1;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS dlm_prewhere_dist;
DROP TABLE IF EXISTS dlm_prewhere_local;

CREATE TABLE dlm_prewhere_local (x UInt64, s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dlm_prewhere_local VALUES (0, 'a'), (1, 'b'), (2, 'c');
CREATE TABLE dlm_prewhere_dist AS dlm_prewhere_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), dlm_prewhere_local);

SELECT count() FROM (EXPLAIN actions = 1, distributed = 1 SELECT x, s FROM dlm_prewhere_dist PREWHERE x = 2 ORDER BY x LIMIT 1) WHERE explain LIKE '%Distributed TopK candidate limit%';
SELECT x, s FROM dlm_prewhere_dist PREWHERE x = 2 ORDER BY x LIMIT 1;
SELECT x, s FROM dlm_prewhere_dist PREWHERE x >= 1 WHERE s != 'b' ORDER BY x LIMIT 1;
SELECT x, s FROM dlm_prewhere_dist PREWHERE x = 2 ORDER BY x LIMIT 1 SETTINGS query_plan_optimize_distributed_lazy_materialization = 0;

DROP TABLE dlm_prewhere_dist;
DROP TABLE dlm_prewhere_local;
