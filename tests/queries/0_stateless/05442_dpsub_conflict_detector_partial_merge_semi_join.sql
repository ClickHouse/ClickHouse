-- With the conflict detector, a semi join is reordered together with the inner joins, and the search may
-- leave it mirrored, as `RIGHT SEMI JOIN`. Partial merge join supports only `LEFT SEMI JOIN`, so the join
-- has to be turned back; it must not fail with `NOT_IMPLEMENTED`.

SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'dpsub,greedy';
SET query_plan_optimize_join_order_conflict_detector = 'c';
SET use_hash_table_stats_for_join_reordering = 0;
SET join_algorithm = 'partial_merge';

DROP TABLE IF EXISTS t_pm_a;
DROP TABLE IF EXISTS t_pm_b;
DROP TABLE IF EXISTS t_pm_c;
CREATE TABLE t_pm_a (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_pm_b (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_pm_c (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_pm_a SELECT number FROM numbers(100);
INSERT INTO t_pm_b SELECT number FROM numbers(1000);
INSERT INTO t_pm_c SELECT number FROM numbers(10);

SELECT count()
FROM t_pm_a LEFT SEMI JOIN t_pm_b ON t_pm_a.k = t_pm_b.k
INNER JOIN t_pm_c ON t_pm_a.k = t_pm_c.k;

SELECT sum(t_pm_a.k), sum(t_pm_b.k)
FROM t_pm_a LEFT SEMI JOIN t_pm_b ON t_pm_a.k = t_pm_b.k
INNER JOIN t_pm_c ON t_pm_a.k = t_pm_c.k;

DROP TABLE t_pm_a;
DROP TABLE t_pm_b;
DROP TABLE t_pm_c;
