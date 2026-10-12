-- Estimated against actual rows per query plan step, from system.processors_profile_log.
-- Requires `log_processors_profiles = 1` for the measured query. Pass the query id as the
-- {query_id:String} parameter, for example:
--   clickhouse-client --param_query_id='...' --queries-file step_estimate_vs_actual.sql
-- The report covers one execution on the node that wrote the log; a query id that was reused
-- merges the executions, and the tasks of a distributed plan log under their own query ids.
--
-- Every processor carries the row estimate of the plan step that created it. A step's actual
-- output is the sum of `output_rows` over its processors whose consumers all belong to other
-- steps; `parent_ids` lists the consumers of a processor, so a chain inside a step is not counted
-- twice. A step is observed only when that boundary is clean: no processor feeds both its own
-- step and another one (`output_rows` counts all of its ports together), and every counted
-- processor has a consumer (a sink without one shows no output to compare). Other steps report
-- their status instead of a measurement.
-- q-error is max(estimate / actual, actual / estimate); 1 when both are zero, NULL when the step is
-- not observed or has no estimate; a zero on one side only is reported as such.
WITH
    log AS
    (
        SELECT id, parent_ids, step_uniq_id, plan_step_name, plan_step_description,
               plan_step_estimated_rows, plan_step_estimate_source, output_rows
        FROM system.processors_profile_log
        WHERE query_id = {query_id:String}
    ),
    consumers AS
    (
        SELECT l.id AS id,
               countIf(c.step_uniq_id = l.step_uniq_id) AS internal_consumers,
               countIf(c.step_uniq_id != l.step_uniq_id) AS external_consumers
        FROM log AS l
        ARRAY JOIN l.parent_ids AS consumer_id
        INNER JOIN log AS c ON c.id = consumer_id
        GROUP BY l.id
    ),
    processors AS
    (
        SELECT l.*, coalesce(k.internal_consumers, 0) AS internal_consumers, coalesce(k.external_consumers, 0) AS external_consumers
        FROM log AS l
        LEFT JOIN consumers AS k ON k.id = l.id
    ),
    steps AS
    (
        SELECT step_uniq_id, any(plan_step_name) AS step, any(plan_step_description) AS description,
               any(plan_step_estimated_rows) AS estimated_rows, any(plan_step_estimate_source) AS source,
               sumIf(output_rows, internal_consumers = 0) AS boundary_rows,
               countIf(internal_consumers > 0 AND external_consumers > 0) AS mixed_processors,
               countIf(internal_consumers = 0 AND external_consumers = 0) AS terminal_processors
        FROM processors
        GROUP BY step_uniq_id
    )
SELECT step, description, estimated_rows, source,
       multiIf(mixed_processors > 0, 'mixed_outputs', terminal_processors > 0, 'terminal',
               estimated_rows IS NULL, 'no_estimate', 'observed') AS status,
       if(status IN ('observed', 'no_estimate'), boundary_rows, NULL) AS actual_rows,
       multiIf(status != 'observed', NULL,
               estimated_rows = 0 AND actual_rows = 0, 1,
               estimated_rows = 0 OR actual_rows = 0, NULL,
               greatest(estimated_rows / actual_rows, actual_rows / estimated_rows)) AS q_error,
       if(status = 'observed' AND (estimated_rows = 0) != (actual_rows = 0), 'zero on one side', '') AS note
FROM steps
ORDER BY q_error DESC NULLS LAST, step;
