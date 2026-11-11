-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- `SET ... = DEFAULT` mutates settings through Context::resetSettingsToDefaultValue, a path
-- separate from applySettingsChanges, so the implication from `distributed_plan_workers_num`
-- must be re-established there too. A `const` pin on `make_distributed_plan` (`readonly` in
-- `system.settings`) vetoes the derivation, so the assertions compare against it.

SET distributed_plan_execute_locally = 1;

SELECT 'derived again after DEFAULT clears an explicit off, unless pinned const';
SET distributed_plan_workers_num = 3;
SET make_distributed_plan = 0;
SELECT getSetting('make_distributed_plan');
SET make_distributed_plan = DEFAULT;
SELECT getSetting('make_distributed_plan') = (SELECT readonly = 0 FROM system.settings WHERE name = 'make_distributed_plan');

SELECT 'stays off after DEFAULT when no workers are configured';
SET distributed_plan_workers_num = 0;
SET make_distributed_plan = DEFAULT;
SELECT getSetting('make_distributed_plan');

SELECT 'DEFAULT in the same statement as the workers num still derives, unless pinned const';
SET distributed_plan_workers_num = 3, make_distributed_plan = DEFAULT;
SELECT getSetting('make_distributed_plan') = (SELECT readonly = 0 FROM system.settings WHERE name = 'make_distributed_plan');
