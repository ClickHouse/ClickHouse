-- Tags: no-old-analyzer, no-parallel
-- no-old-analyzer: make_distributed_plan requires the analyzer.
-- no-parallel: creates a server-global settings profile under a fixed name.

-- A range constraint that forbids the enabled value must veto the `make_distributed_plan`
-- derivation exactly like a `const` pin does, rejecting a worker setting that still wants the
-- plan instead of silently dropping it. Where the environment already pins the setting const for
-- every user (ClickHouse Cloud does), `CREATE SETTINGS PROFILE` below fails with `should not be
-- changed`, which the Cloud test harness treats as a skip.

DROP SETTINGS PROFILE IF EXISTS profile_05625;
CREATE SETTINGS PROFILE profile_05625 SETTINGS make_distributed_plan MAX 0;

SET profile = 'profile_05625';

SELECT 'rejected under the range constraint';
SET distributed_plan_workers_num = 3;
SELECT getSetting('make_distributed_plan'); -- { serverError SETTING_CONSTRAINT_VIOLATION }

SET make_distributed_plan = 1; -- { serverError SETTING_CONSTRAINT_VIOLATION }

SET distributed_plan_workers_num = 0;
DROP SETTINGS PROFILE profile_05625;
