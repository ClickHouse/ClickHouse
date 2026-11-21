-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A derived distributed plan force-adjusts the settings it does not support, but only on the
-- query that derives it: a session whose distributed_plan_workers_num asked for one query's plan
-- must not carry degraded settings into its later ordinary queries. The assertions compare
-- snapshots instead of fixed values, so they hold under randomized harness settings.

SET compile_expressions = 1;

CREATE TEMPORARY TABLE settings_before AS SELECT name, value, changed FROM system.settings;

SET distributed_plan_workers_num = 3;

SELECT 'the derived plan adjusts an explicitly set value';
SELECT getSetting('compile_expressions');

SET distributed_plan_workers_num = 0;

SELECT 'the explicitly set value is unaffected once the workers num goes back to zero';
SELECT getSetting('compile_expressions');

SELECT 'and no other setting keeps a trace of the derived window';
-- Both snapshots go through the same statement shape: the analyzer mutates some per-query
-- settings (e.g. `parallel_replicas_for_cluster_engines`) on a plain SELECT context but not
-- on the inner context of CREATE ... AS SELECT, and a shape mismatch would show that noise.
CREATE TEMPORARY TABLE settings_after AS SELECT name, value, changed FROM system.settings;
SELECT count()
FROM settings_after a
JOIN settings_before b USING (name)
WHERE (a.value != b.value OR a.changed != b.changed) AND name != 'distributed_plan_workers_num';
