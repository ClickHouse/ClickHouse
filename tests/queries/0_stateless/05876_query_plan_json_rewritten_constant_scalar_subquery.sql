-- Tags: no-old-analyzer

-- A constant that comes from a scalar subquery stays linked to the subquery when the analyzer or a
-- query tree pass replaces it with a new constant computed from it: the `EXISTS` result, the affix
-- cut from a `LIKE` pattern, the set an `OR` chain of `equals` becomes, and the range a date
-- comparison becomes. Each new constant carries the ids of the one it replaces.
--
-- Which step reads the value depends on how far the filter is pushed down, so only the existence
-- of a link is checked.

SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_05876_patterns;
DROP TABLE IF EXISTS t_05876_strings;
DROP TABLE IF EXISTS t_05876_dates;

CREATE TABLE t_05876_patterns (pattern String) ENGINE = Memory;
CREATE TABLE t_05876_strings (s String) ENGINE = Memory;
CREATE TABLE t_05876_dates (d Date) ENGINE = Memory;
INSERT INTO t_05876_patterns VALUES ('ab%');
INSERT INTO t_05876_strings VALUES ('abc'), ('xyz'), ('abd');
INSERT INTO t_05876_dates VALUES ('2020-05-01'), ('2021-05-01');

SET log_query_plans = 1;

SELECT count() FROM numbers(10) WHERE number > 0 AND EXISTS (SELECT number FROM numbers(10) WHERE number > 5)
SETTINGS execute_exists_as_scalar_subquery = 1, log_comment = '05876_exists' FORMAT Null;

SELECT count() FROM t_05876_strings WHERE s LIKE assumeNotNull((SELECT any(pattern) FROM t_05876_patterns))
SETTINGS optimize_rewrite_like_perfect_affix = 1, log_comment = '05876_like_affix' FORMAT Null;

SELECT count() FROM numbers(10) WHERE number = 1 OR number = 2 OR number = assumeNotNull((SELECT max(number) FROM numbers(4)))
SETTINGS optimize_min_equality_disjunction_chain_length = 3, log_comment = '05876_equals_chain' FORMAT Null;

SELECT count() FROM t_05876_dates WHERE toYear(d) = assumeNotNull((SELECT toUInt16(2020)))
SETTINGS optimize_time_filter_with_preimage = 1, log_comment = '05876_date_preimage' FORMAT Null;

SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    replaceOne(log_comment, '05876_', '') AS shape,
    length(JSONExtractArrayRaw(toJSONString(query_plan), 'Plans')) AS plans,
    JSONExtractString(toJSONString(query_plan), 'Plans', 2, 'Kind') AS sub_plan_kind,
    countSubstrings(JSONExtractRaw(toJSONString(query_plan), 'Plans', 1), '"SubPlans"') > 0 AS linked
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05876_exists', '05876_like_affix', '05876_equals_chain', '05876_date_preimage')
ORDER BY shape;

DROP TABLE t_05876_patterns;
DROP TABLE t_05876_strings;
DROP TABLE t_05876_dates;
