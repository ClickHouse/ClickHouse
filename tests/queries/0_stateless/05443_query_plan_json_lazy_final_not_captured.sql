-- Tags: no-old-analyzer, no-random-settings, no-random-merge-tree-settings
-- no-random-settings: the randomizer turns off what lazy FINAL needs, and the test is vacuous
-- unless the plan actually uses it.

-- Capturing a plan must not build anything the query did not build for itself.
--
-- `LazyReadReplacingFinalStep` holds a child plan it never constructs while running: only
-- `getChildPlans` builds it, for `EXPLAIN` to render. A capture walking with `getChildPlans`
-- would construct that plan after the query had finished, run index analysis for it, and log its
-- steps as though they had executed. The capture walks with `getBuiltChildPlans` instead, so the
-- step is captured with no children at all.

SET enable_analyzer = 1;                   -- lazy FINAL requires the analyzer
SET query_plan_optimize_lazy_final = 1;    -- off by default
SET min_filtered_ratio_for_lazy_final = 0; -- do not fall back to a regular FINAL

DROP TABLE IF EXISTS t_05443;

CREATE TABLE t_05443 (id UInt64, version UInt64, s String)
ENGINE = ReplacingMergeTree(version) ORDER BY id SETTINGS index_granularity = 256;

-- Overlapping parts, so FINAL has something to do.
SYSTEM STOP MERGES t_05443;
INSERT INTO t_05443 SELECT number, 1, 'a' FROM numbers(5000);
INSERT INTO t_05443 SELECT number, 2, 'b' FROM numbers(3000);

SET log_query_plans = 1;
SELECT count() FROM t_05443 FINAL WHERE s = 'b' SETTINGS log_comment = '05443_lazy_final' FORMAT Null;
SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

-- The step that ran is captured, and it reports no children: the plan it only builds for
-- `EXPLAIN` was never built. A capture that built it would give the step one child, the root of
-- that plan, and the steps below it would be logged as though they had run.
SELECT
    'lazy_final',
    countIf(JSONExtractString(node, 'NodeType') = 'LazyReadReplacingFinal'),
    countIf(JSONExtractString(node, 'NodeType') = 'LazyReadReplacingFinal'
            AND JSONExtractRaw(node, 'Children') = '[]')
FROM
(
    -- One plan is picked before it is split into nodes, so that the row limit applies to plans.
    WITH one_plan AS
    (
        SELECT toJSONString(query_plan) AS plan
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND log_comment = '05443_lazy_final'
          AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    )
    SELECT arrayJoin(JSONExtractArrayRaw(plan, 'Nodes')) AS node FROM one_plan
);

DROP TABLE t_05443;
