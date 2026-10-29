-- Negative and fractional `LIMIT`/`OFFSET` and negative `LIMIT BY` get a distributed plan from the
-- Cascades optimizer. They need the whole result, so they run on one node above the gather and must
-- give exactly the non-distributed result.

DROP TABLE IF EXISTS t_cascades_neg_limit;

CREATE TABLE t_cascades_neg_limit (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS auto_statistics_types = '';
-- a merge between planning and the worker read would invalidate the planned part names
SYSTEM STOP MERGES t_cascades_neg_limit;
INSERT INTO t_cascades_neg_limit SELECT number * 4 FROM numbers(10);

SET enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
-- Without it a rejected plan runs locally and still gives the correct result.
SET distributed_plan_fallback_to_local_execution = 0;
SET param__internal_cascades_cluster_node_count = 4;
-- A large table-size hint makes Cascades always read the table on several nodes.
SET param__internal_join_table_stat_hints = '{"t_cascades_neg_limit": {"cardinality": 100000000, "avg_row_bytes": 8}}';

SELECT '-- LIMIT -4' SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a LIMIT -4 SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a LIMIT -4;
EXPLAIN SELECT a FROM t_cascades_neg_limit ORDER BY a LIMIT -4 SETTINGS explain_query_plan_default = 'legacy';

SELECT '-- LIMIT 2 OFFSET -3' SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a LIMIT 2 OFFSET -3 SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a LIMIT 2 OFFSET -3;

SELECT '-- LIMIT 0.3' SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a LIMIT 0.3 SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a LIMIT 0.3;

SELECT '-- LIMIT 2 OFFSET 0.5' SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a LIMIT 2 OFFSET 0.5 SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a LIMIT 2 OFFSET 0.5;

SELECT '-- LIMIT -1 BY a % 3' SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a LIMIT -1 BY a % 3 SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a LIMIT -1 BY a % 3;

-- The fill adds rows, so these limits apply to the filled result.
SELECT '-- WITH FILL LIMIT -4' SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a WITH FILL STEP 2 LIMIT -4 SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a WITH FILL STEP 2 LIMIT -4;

SELECT '-- WITH FILL LIMIT 2 OFFSET -3' SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a WITH FILL STEP 2 LIMIT 2 OFFSET -3 SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a WITH FILL STEP 2 LIMIT 2 OFFSET -3;

SELECT '-- WITH FILL LIMIT 0.3' SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a WITH FILL STEP 2 LIMIT 0.3 SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a WITH FILL STEP 2 LIMIT 0.3;

SELECT '-- WITH FILL LIMIT 2 OFFSET 0.5' SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a WITH FILL STEP 2 LIMIT 2 OFFSET 0.5 SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a WITH FILL STEP 2 LIMIT 2 OFFSET 0.5;

SELECT '-- WITH FILL LIMIT -1 BY a % 3' SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a WITH FILL STEP 2 LIMIT -1 BY a % 3 SETTINGS make_distributed_plan = 0;
SELECT a FROM t_cascades_neg_limit ORDER BY a WITH FILL STEP 2 LIMIT -1 BY a % 3;

DROP TABLE t_cascades_neg_limit;
