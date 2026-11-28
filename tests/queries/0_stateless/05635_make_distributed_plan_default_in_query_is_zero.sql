-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- In a query's own SETTINGS, `make_distributed_plan = DEFAULT` counts as an explicit 0: it conflicts with a `distributed_plan_workers_num`/`distributed_plan_worker_cluster` that wants a distributed plan, the same as `= 0` would.

SELECT 'plain SELECT: DEFAULT conflicts with a workers num that wants a distributed plan';
SELECT getSetting('make_distributed_plan') SETTINGS distributed_plan_workers_num = 3, distributed_plan_execute_locally = 1, make_distributed_plan = DEFAULT; -- { serverError INVALID_SETTING_VALUE }

SELECT 'UNION (clause on the last arm): same conflict';
SELECT getSetting('make_distributed_plan') UNION ALL SELECT getSetting('make_distributed_plan') SETTINGS distributed_plan_workers_num = 3, distributed_plan_execute_locally = 1, make_distributed_plan = DEFAULT; -- { serverError INVALID_SETTING_VALUE }

SELECT 'EXPLAIN PIPELINE of that SELECT: same conflict';
EXPLAIN PIPELINE SELECT getSetting('make_distributed_plan') SETTINGS distributed_plan_workers_num = 3, distributed_plan_execute_locally = 1, make_distributed_plan = DEFAULT; -- { serverError INVALID_SETTING_VALUE }

SELECT 'EXPLAIN ANALYZE of that SELECT: same conflict';
EXPLAIN ANALYZE SELECT getSetting('make_distributed_plan') SETTINGS distributed_plan_workers_num = 3, distributed_plan_execute_locally = 1, make_distributed_plan = DEFAULT; -- { serverError INVALID_SETTING_VALUE }

SELECT 'a session-level workers num also conflicts with an in-query DEFAULT';
SET distributed_plan_workers_num = 3, distributed_plan_execute_locally = 1;
SELECT getSetting('make_distributed_plan') SETTINGS make_distributed_plan = DEFAULT; -- { serverError INVALID_SETTING_VALUE }
SET distributed_plan_workers_num = DEFAULT, distributed_plan_execute_locally = DEFAULT;

SELECT 'without a workers num or worker cluster, an in-query DEFAULT is just an explicit 0, no error';
SELECT getSetting('make_distributed_plan') SETTINGS make_distributed_plan = DEFAULT;

SELECT 'a bare SET make_distributed_plan = DEFAULT still clears it (not an explicit 0), unlike an in-query SETTINGS clause';
SET make_distributed_plan = 1;
SET make_distributed_plan = DEFAULT;
SET distributed_plan_workers_num = 3, distributed_plan_execute_locally = 1;
SELECT getSetting('make_distributed_plan');
SET distributed_plan_workers_num = DEFAULT, distributed_plan_execute_locally = DEFAULT, make_distributed_plan = DEFAULT;
