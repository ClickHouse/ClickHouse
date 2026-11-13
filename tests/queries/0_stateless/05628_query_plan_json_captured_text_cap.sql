-- Tags: no-old-analyzer

-- `query_plan_max_step_description_length` bounds every piece of text the capture stores, and the
-- cut is moved back to a UTF-8 boundary.
--
-- `05325` covers the cap on a step's `Details`. This covers the two things that cap did not reach.
-- A folded constant becomes the name of the column it produces, so it lands in `Output`, which a
-- query can make arbitrarily large: the capture runs with the memory tracker blocked and the result
-- is stored on a `system.query_log` row, so an unbounded carrier is a cheap way to make a server
-- store a very expensive row. And a cut by bytes can land inside a multi-byte character, putting
-- half of one into a `JSON` column.

SET log_query_plans = 1;

-- A megabyte-long constant, under a 64-byte cap.
SELECT repeat('0123456789', 100000) AS x FROM numbers(1)
SETTINGS query_plan_max_step_description_length = 64, log_comment = '05628_output' FORMAT Null;

-- The predicate ends in a long run of two-byte characters, so consecutive caps land inside it and
-- a cut by bytes would split one.
SELECT count() FROM numbers(10) WHERE toString(number) != 'ééééééééééééééééééé' AND '05628_utf8' != ''
SETTINGS query_plan_max_step_description_length = 60 FORMAT Null;

SELECT count() FROM numbers(10) WHERE toString(number) != 'ééééééééééééééééééé' AND '05628_utf8' != ''
SETTINGS query_plan_max_step_description_length = 61 FORMAT Null;

SELECT count() FROM numbers(10) WHERE toString(number) != 'ééééééééééééééééééé' AND '05628_utf8' != ''
SETTINGS query_plan_max_step_description_length = 62 FORMAT Null;

SELECT count() FROM numbers(10) WHERE toString(number) != 'ééééééééééééééééééé' AND '05628_utf8' != ''
SETTINGS query_plan_max_step_description_length = 63 FORMAT Null;

SET log_query_plans = 0;
SYSTEM FLUSH LOGS query_log;

-- The constant does not reach the row: the whole document stays small, no `Output` entry exceeds
-- the cap, and one reaches it -- otherwise the constant was not large enough to be cut and the
-- assertion would hold vacuously.
SELECT
    'output',
    max(length(plan)) < 4096,
    max(length(output_column)) <= 64,
    countIf(length(output_column) = 64) > 0
FROM
(
    SELECT
        plan,
        arrayJoin(JSONExtract(plan, 'Output', 'Array(String)')) AS output_column
    FROM
    (
        SELECT toJSONString(query_plan) AS plan
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
          AND position(query, '05628_output') > 0
    )
);

-- Every stored document is valid UTF-8, no captured line exceeds the cap it was written under, and
-- at least one line was pulled back below its cap -- a cut by bytes always lands on the cap, so
-- this is the boundary adjustment itself.
SELECT
    'utf8_boundary',
    min(isValidUTF8(plan)) = 1,
    max(length(detail)) <= max(cap),
    countIf(length(detail) = cap - 1) > 0
FROM
(
    SELECT
        plan,
        cap,
        arrayJoin(arrayFlatten(arrayMap(
            node -> JSONExtract(node, 'Details', 'Array(String)'),
            JSONExtractArrayRaw(plan, 'Nodes')))) AS detail
    FROM
    (
        SELECT
            toJSONString(query_plan) AS plan,
            Settings['query_plan_max_step_description_length']::UInt64 AS cap
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
          AND position(query, '05628_utf8') > 0
    )
);
