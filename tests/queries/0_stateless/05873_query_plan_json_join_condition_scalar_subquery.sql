-- Tags: no-old-analyzer

-- A scalar subquery in the condition of a join is linked to the join step that evaluates it.
--
-- The links are recorded after the plan is optimized, when the logical join has become a physical
-- step: a hash join keeps the part of `ON` that is not equality of keys in its `TableJoin`, as the
-- mixed condition, and a block nested loop join keeps the whole condition as its predicate.

SET enable_parallel_replicas = 0;
-- The kind of join the plan uses must not depend on the randomized settings.
SET query_plan_join_swap_table = 'false';
SET allow_block_nested_loop_join = 1;

DROP TABLE IF EXISTS t_05873_l;
DROP TABLE IF EXISTS t_05873_r;
DROP TABLE IF EXISTS t_05873_bounds;

CREATE TABLE t_05873_l (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_05873_r (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_05873_bounds (v UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_05873_l SELECT number, number FROM numbers(10);
INSERT INTO t_05873_r SELECT number, number * 2 FROM numbers(10);
INSERT INTO t_05873_bounds SELECT number FROM numbers(10);

SET log_query_plans = 1;

SELECT count() FROM t_05873_l AS l LEFT JOIN t_05873_r AS r
    ON l.k = r.k AND l.v + r.v > (SELECT avg(v) FROM t_05873_bounds)
SETTINGS join_algorithm = 'hash', log_comment = '05873_hash_mixed' FORMAT Null;

SELECT count() FROM t_05873_l AS l LEFT JOIN t_05873_r AS r
    ON l.v < r.v + (SELECT min(v) FROM t_05873_bounds)
SETTINGS join_algorithm = 'direct,parallel_hash,hash', log_comment = '05873_block_nested_loop' FORMAT Null;

SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

-- For each query: the number of plans, and the types of the nodes of the query's plan that link
-- to a sub-plan.
SELECT
    replaceOne(log_comment, '05873_', '') AS shape,
    length(JSONExtractArrayRaw(toJSONString(query_plan), 'Plans')) AS plans,
    JSONExtractString(toJSONString(query_plan), 'Plans', 2, 'Kind') AS sub_plan_kind,
    arrayFilter(
        t -> t != '',
        arrayMap(
            n -> if(position(n, '"SubPlans"') > 0, JSONExtractString(n, 'NodeType'), ''),
            JSONExtractArrayRaw(toJSONString(query_plan), 'Plans', 1, 'Nodes'))) AS consumers
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05873_hash_mixed', '05873_block_nested_loop')
ORDER BY shape;

DROP TABLE t_05873_l;
DROP TABLE t_05873_r;
DROP TABLE t_05873_bounds;
