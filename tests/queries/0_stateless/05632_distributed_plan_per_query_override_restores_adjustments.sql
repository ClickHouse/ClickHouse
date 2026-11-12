-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A per-query SETTINGS override that turns the derived plan off for just this query must restore
-- the settings it force-adjusted, the same as a session-level SET does: the query runs on a fresh
-- context copied from the session, and that copy must still know what to restore.

SET compile_expressions = 1;
SET distributed_plan_workers_num = 3;

SELECT 'the session derives the plan and overrides the explicit value';
SELECT getSetting('compile_expressions');

SELECT 'a per-query override that turns the plan off for this query restores it';
SELECT getSetting('compile_expressions') SETTINGS distributed_plan_workers_num = 0;

SELECT 'the session itself is unaffected by the per-query override';
SELECT getSetting('compile_expressions');
