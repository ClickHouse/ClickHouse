-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Stateless Workers only run the stages of a distributed plan, so a non-zero
-- `distributed_plan_workers_num` enables `make_distributed_plan` on its own (issue #114501).
-- The environment may pin `make_distributed_plan` with a `const` constraint (ClickHouse Cloud
-- does), and the pin vetoes the derivation, so the assertions compare against the pin
-- (`readonly` in `system.settings`) instead of expecting fixed values. Everything runs locally
-- so that the implied plan never dials a Worker.

-- A global GROUP BY limit cannot be enforced once aggregation is split per bucket, and the test
-- server profile sets one, so clear it for the aggregation below.
SET distributed_plan_execute_locally = 1, max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_dp_workers;
CREATE TABLE t_dp_workers (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dp_workers SELECT number, number FROM numbers(100000);

SELECT 'off while the workers num is zero';
SELECT getSetting('make_distributed_plan');

SELECT 'implied by a per-query workers num unless pinned const';
SELECT getSetting('make_distributed_plan') = (SELECT readonly = 0 FROM system.settings WHERE name = 'make_distributed_plan') SETTINGS distributed_plan_workers_num = 3;

SELECT 'the implied plan adjusts the settings it does not support, unless pinned const';
SELECT getSetting('compile_expressions') = (SELECT readonly != 0 FROM system.settings WHERE name = 'make_distributed_plan') SETTINGS distributed_plan_workers_num = 3, compile_expressions = 1;

SELECT 'the query distributes with make_distributed_plan left alone';
-- The workers num rides on the explained query itself: the wrapper query has to stay local,
-- because a distributed wrapper could not serialize its read from the EXPLAIN table function
-- for remote execution. sum() and not a bare count() so the trivial-count optimization cannot
-- fold the plan away.
SELECT (count() > 0) = (SELECT readonly = 0 FROM system.settings WHERE name = 'make_distributed_plan')
FROM (EXPLAIN PIPELINE SELECT sum(v) FROM t_dp_workers SETTINGS distributed_plan_workers_num = 3)
WHERE explain LIKE '%ReadFromDistributedPlanSource%';

DROP TABLE t_dp_workers;

SELECT 'implied by a session workers num unless pinned const';
SET distributed_plan_workers_num = 3;
SELECT getSetting('make_distributed_plan') = (SELECT readonly = 0 FROM system.settings WHERE name = 'make_distributed_plan');

SELECT 'off again when the workers num goes back to zero';
SET distributed_plan_workers_num = 0;
SELECT getSetting('make_distributed_plan');

SELECT 'an explicit value wins over the implication';
SET make_distributed_plan = 0;
SET distributed_plan_workers_num = 5;
SELECT getSetting('make_distributed_plan');
