-- Tags: no-parallel
-- - no-parallel - the query result cache is shared
-- A plan that `make_distributed_plan` really splits into fragments must not ship the
-- non-serializable `StreamInQueryResultCacheStep` of a subquery to a worker, even with an explicit
-- subquery `SETTINGS use_query_cache = 1`: `QueryPlan::convertToDistributed` drops the step before
-- the split, so the query succeeds and no subquery cache entry is created.
-- The complementary case - a plan that falls back to one local stage and keeps populating the cache -
-- is covered by `05024_query_result_cache_subquery_distributed_plan_local_fallback`.

SYSTEM DROP QUERY CACHE;

DROP TABLE IF EXISTS t_qrc_dp;
CREATE TABLE t_qrc_dp (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_qrc_dp SELECT number FROM numbers(1000);

-- Subquery caching is a Planner feature: with the old analyzer no subquery cache entry is created at
-- all. Pin the analyzer, because some CI jobs run the whole suite with `enable_analyzer = 0`.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 1;
-- `make_distributed_plan` rejects an aggregation with a `max_rows_to_group_by` limit, which some CI
-- profiles set.
SET max_rows_to_group_by = 0;

-- 0 + 1 + ... + 9 = 45.
SELECT sum(k) FROM (SELECT k FROM t_qrc_dp WHERE k < 10 SETTINGS use_query_cache = 1);

-- Leave distributed-plan mode before querying the local system table.
SET make_distributed_plan = 0;
SELECT count() FROM system.query_cache WHERE is_subquery = 1 AND query LIKE '%' || currentDatabase() || '.t_qrc_dp WHERE k < 10%';

DROP TABLE t_qrc_dp;
SYSTEM DROP QUERY CACHE;
