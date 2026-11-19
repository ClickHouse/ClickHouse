-- Lazy materialization over joins with a function of the side an outer join can leave unmatched that throws for a
-- default value, although it is cheap: `toBool` of an empty string. Recomputed above the LIMIT under the join's mask, it
-- would be evaluated on the unmatched rows too, so it is computed where the plan computes it. The results are compared
-- with the optimization disabled.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
-- Pinned so that the join keeps the order it is written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64, heavy String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, s String, payload String) ENGINE = MergeTree ORDER BY k;

INSERT INTO l VALUES (1, 'l1'), (2, 'l2');
INSERT INTO r VALUES (1, 'true', 'payload');

SELECT l.k, l.heavy, t.b, t.payload FROM l LEFT JOIN (SELECT k, toBool(s) AS b, payload FROM r) AS t ON l.k = t.k ORDER BY l.k LIMIT 2
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.heavy, t.b, t.payload FROM l LEFT JOIN (SELECT k, toBool(s) AS b, payload FROM r) AS t ON l.k = t.k ORDER BY l.k LIMIT 2
SETTINGS query_plan_lazy_materialization_for_join = 0;

DROP TABLE l;
DROP TABLE r;
