-- Lazy materialization for `JOIN` rewrites every `UNION ALL` arm separately and puts the rewritten fragments
-- back into the plan. The fragments must not copy the resources of the whole plan, or the plan would append
-- its resources to itself with every rewrite, and their number would grow exponentially with the arms.

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64, heavy String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE r (k UInt64, rheavy String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO l SELECT number, repeat('a', 100) FROM numbers(100);
INSERT INTO r SELECT number, repeat('b', 100) FROM numbers(100);

SET query_plan_lazy_materialization_for_join = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET optimize_read_in_order = 0;
SET join_algorithm = 'hash';
SET max_memory_usage = 134217728;

SELECT count(), sum(length(heavy)), sum(length(rheavy)) FROM (
    (SELECT l.k, l.heavy, r.rheavy FROM l INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 1)
    UNION ALL (SELECT l.k, l.heavy, r.rheavy FROM l INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 1)
    UNION ALL (SELECT l.k, l.heavy, r.rheavy FROM l INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 1)
    UNION ALL (SELECT l.k, l.heavy, r.rheavy FROM l INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 1)
    UNION ALL (SELECT l.k, l.heavy, r.rheavy FROM l INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 1)
    UNION ALL (SELECT l.k, l.heavy, r.rheavy FROM l INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 1)
    UNION ALL (SELECT l.k, l.heavy, r.rheavy FROM l INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 1)
    UNION ALL (SELECT l.k, l.heavy, r.rheavy FROM l INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 1)
    UNION ALL (SELECT l.k, l.heavy, r.rheavy FROM l INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 1)
    UNION ALL (SELECT l.k, l.heavy, r.rheavy FROM l INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 1)
    UNION ALL (SELECT l.k, l.heavy, r.rheavy FROM l INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 1)
    UNION ALL (SELECT l.k, l.heavy, r.rheavy FROM l INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 1)
);

DROP TABLE l;
DROP TABLE r;
