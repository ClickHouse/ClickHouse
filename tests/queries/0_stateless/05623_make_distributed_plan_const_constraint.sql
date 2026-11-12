-- Tags: no-old-analyzer, no-parallel
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- no-parallel: creates a server-global settings profile under a fixed name.

-- A `const` constraint on make_distributed_plan must veto the value derived from
-- distributed_plan_workers_num, not just explicit changes. Once the constraint is active, a worker
-- setting that still wants the plan is rejected instead of silently dropped.

DROP SETTINGS PROFILE IF EXISTS profile_05623;
CREATE SETTINGS PROFILE profile_05623 SETTINGS make_distributed_plan CONST;

SELECT 'derived while not pinned const';
SET distributed_plan_workers_num = 3;
SELECT getSetting('make_distributed_plan');

SELECT 'rejected once the const profile arrives';
SET profile = 'profile_05623';
SELECT getSetting('make_distributed_plan'); -- { serverError SETTING_CONSTRAINT_VIOLATION }

-- Reset first: the label query would otherwise want and be refused too.
SET distributed_plan_workers_num = 0;
SELECT 'still rejected under the constraint';
SET distributed_plan_workers_num = 5;
SELECT getSetting('make_distributed_plan'); -- { serverError SETTING_CONSTRAINT_VIOLATION }

SET make_distributed_plan = 1; -- { serverError SETTING_CONSTRAINT_VIOLATION }

SET distributed_plan_workers_num = 0;
DROP SETTINGS PROFILE profile_05623;
