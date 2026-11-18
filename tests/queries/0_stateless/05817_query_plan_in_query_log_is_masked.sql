-- Tags: no-old-analyzer

-- `system.query_log.query_plan` is masked like the `query` column of the same row.
--
-- The rules in `tests/config/config.d/query_masking_rules.xml` replace `TOPSECRET.TOPSECRET` with
-- `[hidden]`.
--
-- A query in which a rule matches stores no plan: constant folding could carry the masked literal
-- into the plan in a form no rule matches anymore, like `lower` of it.
--
-- A query in which no rule matches stores its plan, and every text of the plan is masked: a value
-- the plan holds can still match, once folding has assembled it or a scalar subquery has read it.
-- Such values are put where the plan renders them: a filter constant, an `IN` list, an output
-- constant, a primary key condition, and the input columns a join reports in its statistics.
--
-- A query with secret arguments stores no plan either: see the `no-fasttest` test next to this one.

DROP TABLE IF EXISTS t_plan_masked;
CREATE TABLE t_plan_masked (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_plan_masked VALUES ('a'), ('b'), (concat('find_me_TOPSECRET', '=TOPSECRET'));

SET log_query_plans = 1;

-- No plan: a rule matches the query.
SELECT count() FROM numbers(10) WHERE toString(number) != 'find_me_TOPSECRET=TOPSECRET'
SETTINGS log_comment = 'mask_declined_literal' FORMAT Null;

SELECT lower('find_me_TOPSECRET=TOPSECRET') FROM numbers(1)
SETTINGS log_comment = 'mask_declined_folded' FORMAT Null;

-- A plan, masked.
SELECT count() FROM numbers(10) WHERE toString(number) != concat('find_me_TOPSECRET', '=TOPSECRET')
SETTINGS log_comment = 'mask_masked_filter' FORMAT Null;

SELECT count() FROM numbers(10) WHERE toString(number) IN ('1', concat('find_me_TOPSECRET', '=TOPSECRET'))
SETTINGS log_comment = 'mask_masked_in' FORMAT Null;

SELECT concat('find_me_TOPSECRET', '=TOPSECRET') FROM numbers(1)
SETTINGS log_comment = 'mask_masked_output' FORMAT Null;

SELECT count() FROM t_plan_masked WHERE s = concat('find_me_TOPSECRET', '=TOPSECRET')
SETTINGS log_comment = 'mask_masked_index' FORMAT Null;

SELECT count() FROM t_plan_masked WHERE s = (SELECT max(s) FROM t_plan_masked)
SETTINGS log_comment = 'mask_masked_scalar_subquery' FORMAT Null;

SELECT * FROM (SELECT number AS k, concat('find_me_TOPSECRET', '=TOPSECRET') FROM numbers(3)) AS l
INNER JOIN (SELECT number AS k FROM numbers(3)) AS r USING (k)
SETTINGS log_comment = 'mask_masked_join' FORMAT Null;

-- No plan: a rule matches a query parameter. Every parameter of the session is checked, so this
-- comes last: the parameter stays set, and would decline the queries after it too.
SET param_p = 'find_me_TOPSECRET=TOPSECRET';
SELECT count() FROM numbers(10) WHERE toString(number) != {p:String}
SETTINGS log_comment = 'mask_declined_parameter' FORMAT Null;

-- A query that does not use the parameter is declined as well.
SELECT count() FROM numbers(10)
SETTINGS log_comment = 'mask_declined_session_parameter' FORMAT Null;

SET log_query_plans = 0;
SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    count(),
    -- The plan is stored...
    countIf(position(toJSONString(query_plan), '"Nodes"') > 0),
    -- ...holds the value only in its masked form...
    countIf(position(toJSONString(query_plan), 'find_me_[hidden]') > 0),
    -- ...and never the original.
    countIf(position(toJSONString(query_plan), 'TOPSECRET=TOPSECRET') > 0)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(log_comment, 'mask_')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_plan_masked;
