-- Tags: no-old-analyzer, no-parallel
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- no-parallel: the constraint case below creates a server-global settings profile under a fixed name.

-- Whether a distributed query plan is wanted - `make_distributed_plan = 1`,
-- `distributed_plan_workers_num > 0`, or a non-empty `distributed_plan_worker_cluster` - and
-- whether one is refused - `make_distributed_plan = 0`, `distributed_plan_workers_num = 0`, or a
-- settings constraint forbidding `make_distributed_plan = 1` - are judged together, once per
-- query, from their explicitly set values only (an unset setting counts toward neither side).
-- Wanting and refusing at once is rejected with an error instead of silently resolved one way, so
-- the result does not depend on which of the two settings was set first.

SELECT 'neither wants nor refuses: the plan stays off';
SELECT getSetting('make_distributed_plan');

SELECT 'wants only, via the workers num: the plan turns on';
SELECT getSetting('make_distributed_plan') SETTINGS distributed_plan_workers_num = 3;

SELECT 'wants only, via the worker cluster: the plan turns on';
SELECT getSetting('make_distributed_plan') SETTINGS distributed_plan_worker_cluster = 'some_cluster';

SELECT 'refuses only, via an explicit off: the plan stays off, no error';
SELECT getSetting('make_distributed_plan') SETTINGS make_distributed_plan = 0;

SELECT 'refuses only, via a zero workers num: the plan stays off, no error';
SELECT getSetting('make_distributed_plan') SETTINGS distributed_plan_workers_num = 0;

SELECT 'wants via the workers num, refuses via an explicit off: rejected';
SELECT getSetting('make_distributed_plan') SETTINGS distributed_plan_workers_num = 3, make_distributed_plan = 0; -- { serverError INVALID_SETTING_VALUE }

SELECT 'wants via make_distributed_plan, refuses via a zero workers num: rejected';
SELECT getSetting('make_distributed_plan') SETTINGS make_distributed_plan = 1, distributed_plan_workers_num = 0; -- { serverError INVALID_SETTING_VALUE }

SELECT 'wants via the worker cluster, refuses via a zero workers num: rejected';
SELECT getSetting('make_distributed_plan') SETTINGS distributed_plan_worker_cluster = 'some_cluster', distributed_plan_workers_num = 0; -- { serverError INVALID_SETTING_VALUE }

DROP SETTINGS PROFILE IF EXISTS profile_05628;
CREATE SETTINGS PROFILE profile_05628 SETTINGS make_distributed_plan CONST;
SET profile = 'profile_05628';

SELECT 'wants via the workers num, refuses via a settings constraint: rejected';
SELECT getSetting('make_distributed_plan') SETTINGS distributed_plan_workers_num = 3; -- { serverError SETTING_CONSTRAINT_VIOLATION }

SET distributed_plan_workers_num = 0;
DROP SETTINGS PROFILE profile_05628;
