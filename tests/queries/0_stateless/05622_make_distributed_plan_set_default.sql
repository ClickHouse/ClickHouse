-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- `SET ... = DEFAULT` mutates settings through Context::resetSettingsToDefaultValue, a path
-- separate from applySettingsChanges, so the implication from `distributed_plan_workers_num`
-- must be re-established there too.

SET distributed_plan_execute_locally = 1;

SELECT 'derived again after DEFAULT clears an explicit off';
-- The three SETs run back to back, with no query between them: a bare SET only changes what a
-- later query would want, so the conflicting intermediate state (an explicit off while the
-- workers num still wants the plan) is never itself checked.
SET distributed_plan_workers_num = 3;
SET make_distributed_plan = 0;
SET make_distributed_plan = DEFAULT;
SELECT getSetting('make_distributed_plan');

SELECT 'stays off after DEFAULT when no workers are configured';
SET distributed_plan_workers_num = 0;
SET make_distributed_plan = DEFAULT;
SELECT getSetting('make_distributed_plan');

SELECT 'DEFAULT in the same statement as the workers num still derives';
SET distributed_plan_workers_num = 3, make_distributed_plan = DEFAULT;
SELECT getSetting('make_distributed_plan');
