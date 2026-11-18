-- Tags: no-old-analyzer, no-parallel-replicas

-- A scalar subquery whose own `SETTINGS` make it run as a distributed plan is not captured, while
-- the query around it, which runs locally, still is. One that asks for a distributed plan but falls
-- back to local execution runs locally, and is captured as usual.
--
-- Building the pipeline of a distributed plan replaces its steps with one reading from the
-- distributed executor, so all that could be recorded is that one step. A distributed query's own
-- plan is not captured for the same reason (see `05218_query_plan_json_distributed_plan`).

DROP TABLE IF EXISTS t_05816;

CREATE TABLE t_05816 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05816 SELECT number, number * 2 FROM numbers(1000);

SET log_query_plans = 1;

SELECT count() FROM t_05816
WHERE v > (SELECT avg(v) FROM t_05816 WHERE k % 7 = 0
    SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1)
    SETTINGS enable_parallel_replicas = 0, log_comment = '05816_distributed_scalar' FORMAT Null;

-- The same subquery run locally, which must be captured.
SELECT count() FROM t_05816
WHERE v > (SELECT avg(v) FROM t_05816 WHERE k % 7 = 0)
    SETTINGS enable_parallel_replicas = 0, log_comment = '05816_local_scalar' FORMAT Null;

SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    replaceOne(log_comment, '05816_', '') AS shape,
    JSONExtractString(toJSONString(query_plan), 'Plans', 1, 'Kind') AS query_kind,
    length(JSONExtractArrayRaw(toJSONString(query_plan), 'Plans')) AS plans,
    countSubstrings(toJSONString(query_plan), '"SubPlans"') AS links
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05816_distributed_scalar', '05816_local_scalar')
    AND position(query, 'log_comment') > 0
ORDER BY shape;

DROP TABLE t_05816;
