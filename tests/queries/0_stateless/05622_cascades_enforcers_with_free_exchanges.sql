-- An enforcer of the cost-based distributed optimizer answers only the required properties that it was made
-- for, so a sort or a keyed exchange is never selected as its own input. A gather and a scatter can still
-- lead to each other when an exchange costs nothing; plan extraction then excludes the chain that it builds.
-- Each query must get a plan with each cost configuration. The plans are not checked here: with free
-- exchanges many alternatives cost the same.

DROP TABLE IF EXISTS t_enforcers;
CREATE TABLE t_enforcers (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_enforcers SELECT number % 100, number FROM numbers(10000);

SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET param__internal_cascades_cluster_node_count = 4;

SELECT '-- default costs';
SELECT count() > 0 FROM (EXPLAIN SELECT k, v FROM t_enforcers ORDER BY v);
SELECT count() > 0 FROM (EXPLAIN SELECT k, v FROM t_enforcers ORDER BY v LIMIT 10);
SELECT count() > 0 FROM (EXPLAIN SELECT k, v, sum(v) OVER (PARTITION BY k ORDER BY v) FROM t_enforcers ORDER BY k, v);
SELECT count() > 0 FROM (EXPLAIN SELECT k, s, sum(s) OVER (PARTITION BY k) FROM (SELECT k, v, sum(v) OVER (PARTITION BY k, v) AS s FROM t_enforcers));
SELECT count() > 0 FROM (EXPLAIN SELECT l.k, r.v FROM t_enforcers AS l INNER JOIN t_enforcers AS r ON l.k = r.k ORDER BY l.k);
SELECT count() > 0 FROM (EXPLAIN SELECT DISTINCT k FROM (SELECT DISTINCT k, v FROM t_enforcers) ORDER BY k LIMIT 10);

SELECT '-- free exchanges';
SET param__internal_cascades_cost_config = '{"network_weight":0,"sequential_weight":0,"exchange_fixed_overhead":0}';
SELECT count() > 0 FROM (EXPLAIN SELECT k, v FROM t_enforcers ORDER BY v);
SELECT count() > 0 FROM (EXPLAIN SELECT k, v FROM t_enforcers ORDER BY v LIMIT 10);
SELECT count() > 0 FROM (EXPLAIN SELECT k, v, sum(v) OVER (PARTITION BY k ORDER BY v) FROM t_enforcers ORDER BY k, v);
SELECT count() > 0 FROM (EXPLAIN SELECT k, s, sum(s) OVER (PARTITION BY k) FROM (SELECT k, v, sum(v) OVER (PARTITION BY k, v) AS s FROM t_enforcers));
SELECT count() > 0 FROM (EXPLAIN SELECT l.k, r.v FROM t_enforcers AS l INNER JOIN t_enforcers AS r ON l.k = r.k ORDER BY l.k);
SELECT count() > 0 FROM (EXPLAIN SELECT DISTINCT k FROM (SELECT DISTINCT k, v FROM t_enforcers) ORDER BY k LIMIT 10);

SELECT '-- all costs are zero';
SET param__internal_cascades_cost_config = '{"work_weight":0,"network_weight":0,"sequential_weight":0}';
SELECT count() > 0 FROM (EXPLAIN SELECT k, v FROM t_enforcers ORDER BY v);
SELECT count() > 0 FROM (EXPLAIN SELECT k, v FROM t_enforcers ORDER BY v LIMIT 10);
SELECT count() > 0 FROM (EXPLAIN SELECT k, v, sum(v) OVER (PARTITION BY k ORDER BY v) FROM t_enforcers ORDER BY k, v);
SELECT count() > 0 FROM (EXPLAIN SELECT k, s, sum(s) OVER (PARTITION BY k) FROM (SELECT k, v, sum(v) OVER (PARTITION BY k, v) AS s FROM t_enforcers));
SELECT count() > 0 FROM (EXPLAIN SELECT l.k, r.v FROM t_enforcers AS l INNER JOIN t_enforcers AS r ON l.k = r.k ORDER BY l.k);
SELECT count() > 0 FROM (EXPLAIN SELECT DISTINCT k FROM (SELECT DISTINCT k, v FROM t_enforcers) ORDER BY k LIMIT 10);

DROP TABLE t_enforcers;
