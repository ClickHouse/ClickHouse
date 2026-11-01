-- Lazy materialization over joins with a direct join, which reads the right table from a prepared lookup that has to stay
-- the immediate child of the join: a copy of a carried column of the right side added above the lookup made the direct
-- join impossible. Such a join is read as it is. The results are compared with the optimization disabled.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 100;
SET join_algorithm = 'direct';
-- Pinned so that the join keeps the order it is written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, rpayload String) ENGINE = MergeTree ORDER BY k;

INSERT INTO l SELECT number, concat('l', toString(number)) FROM numbers(100);
INSERT INTO r SELECT number * 2, concat('r', toString(number * 2)) FROM numbers(50);

SELECT '-- INNER JOIN';
SELECT l.k, l.payload, r.rpayload FROM l ALL INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 3 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.payload, r.rpayload FROM l ALL INNER JOIN r ON l.k = r.k ORDER BY l.k LIMIT 3 SETTINGS query_plan_lazy_materialization_for_join = 0;

SELECT '-- LEFT JOIN';
SELECT l.k, l.payload, r.rpayload FROM l ALL LEFT JOIN r ON l.k = r.k ORDER BY l.k LIMIT 3 SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT l.k, l.payload, r.rpayload FROM l ALL LEFT JOIN r ON l.k = r.k ORDER BY l.k LIMIT 3 SETTINGS query_plan_lazy_materialization_for_join = 0;

DROP TABLE l;
DROP TABLE r;
