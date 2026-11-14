-- Tags: no-parallel
-- - no-parallel - the query result cache is shared
-- With the Cascades optimizer, `QueryPlan::optimize` clones every step of a plan accepted by
-- `make_distributed_plan`. The `StreamInQueryResultCacheStep` of a subquery with `use_query_cache = 1`
-- cannot be cloned, so it must be removed as soon as the plan is accepted for distributed execution,
-- before the optimization, and not only when the plan is split into fragments.

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_qrc_cascades;
CREATE TABLE t_qrc_cascades (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_qrc_cascades SELECT number FROM numbers(10);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
-- `make_distributed_plan` rejects an aggregation with a `max_rows_to_group_by` limit, which some CI
-- profiles set.
SET max_rows_to_group_by = 0;

-- 0 + 1 + ... + 9 = 45.
SELECT sum(k)
FROM (SELECT k FROM t_qrc_cascades WHERE k < 10 SETTINGS use_query_cache = 1)
SETTINGS make_distributed_plan = 1,
         enable_cascades_optimizer = 1,
         distributed_plan_execute_locally = 1,
         distributed_plan_workers_num = 2;

DROP TABLE t_qrc_cascades;
SYSTEM DROP QUERY CACHE;
