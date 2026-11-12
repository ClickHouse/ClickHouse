-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- Stateless Workers only run the stages of a distributed plan, so a non-zero
-- `distributed_plan_workers_num` enables `make_distributed_plan` on its own (issue #114501).
-- Everything runs locally so that the implied plan never dials a Worker.

-- A global GROUP BY limit cannot be enforced once aggregation is split per bucket, and the test
-- server profile sets one, so clear it for the aggregation below.
SET distributed_plan_execute_locally = 1, max_rows_to_group_by = 0;

SELECT 'off while the workers num is zero';
SELECT getSetting('make_distributed_plan');

SELECT 'implied by a per-query workers num';
SELECT getSetting('make_distributed_plan') SETTINGS distributed_plan_workers_num = 3;

SELECT 'the implied plan adjusts the settings it does not support';
SELECT getSetting('compile_expressions') SETTINGS distributed_plan_workers_num = 3, compile_expressions = 1;

SELECT 'implied by a session workers num';
SET distributed_plan_workers_num = 3;
SELECT getSetting('make_distributed_plan');

SELECT 'off again when the workers num goes back to zero';
SET distributed_plan_workers_num = 0;
SELECT getSetting('make_distributed_plan');

SELECT 'an explicit make_distributed_plan = 0 conflicts with a non-zero workers num';
SET make_distributed_plan = 0;
SET distributed_plan_workers_num = 5;
SELECT getSetting('make_distributed_plan'); -- { serverError INVALID_SETTING_VALUE }

SELECT 'DEFAULT clears an explicit off, so the workers num derives it again';
SET make_distributed_plan = DEFAULT;
SELECT getSetting('make_distributed_plan');

SELECT 'the derivation and its adjustments show up in query_log for that query only';
-- Pinned explicitly, before the session-level workers num below: the harness randomizes this
-- setting, and the point is to see it flip from the session's own value to the adjusted one.
SET distributed_plan_workers_num = 0, make_distributed_plan = DEFAULT, compile_expressions = 1;
SET distributed_plan_workers_num = 3;
SELECT 1 SETTINGS log_comment = '05621_query_log_probe_on';
SET distributed_plan_workers_num = 0;
SELECT 1 SETTINGS log_comment = '05621_query_log_probe_off';
SYSTEM FLUSH LOGS query_log;
SELECT Settings['make_distributed_plan'], Settings['compile_expressions']
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '05621_query_log_probe_on'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT 'and a later query with the workers num back to zero logs neither override';
SELECT Settings['make_distributed_plan'], Settings['compile_expressions']
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '05621_query_log_probe_off'
ORDER BY event_time_microseconds DESC LIMIT 1;
