-- The cap on the groups of a trivial `GROUP BY ... LIMIT` is not used in a subquery that disables `make_distributed_plan`
-- while the enclosing query enables it: `make_distributed_plan` cannot split an aggregation with a cap.

DROP TABLE IF EXISTS t;
DROP VIEW IF EXISTS v;

CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t SELECT number % 20 FROM numbers(200);

CREATE VIEW v AS SELECT k, count() AS c FROM t GROUP BY k LIMIT 5 SETTINGS make_distributed_plan = 0;

SET optimize_trivial_group_by_limit_query = 1;
SET make_distributed_plan = 1, distributed_plan_execute_locally = 1, distributed_plan_fallback_to_local_execution = 0;
SET enable_cascades_optimizer = 0, max_rows_to_group_by = 0, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SELECT 'without aggregates', count() FROM (SELECT k FROM t GROUP BY k LIMIT 5 SETTINGS make_distributed_plan = 0);
SELECT 'without aggregates, DEFAULT', count() FROM (SELECT k FROM t GROUP BY k LIMIT 5 SETTINGS make_distributed_plan = DEFAULT);
SELECT 'with aggregates', count(), sum(c) FROM (SELECT k, count() AS c FROM t GROUP BY k LIMIT 5 SETTINGS make_distributed_plan = 0);
SELECT 'view', count(), sum(c) FROM v;

DROP VIEW v;
DROP TABLE t;
