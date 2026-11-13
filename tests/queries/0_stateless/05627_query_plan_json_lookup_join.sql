-- Tags: no-old-analyzer

-- A join whose right side is a `Join`-engine table is planned as a `JoinStepLogicalLookup` holding
-- the plan that reads it, and optimization turns the pair into a `FilledJoin` against the prepared
-- table. The plan is captured after optimization, so what reaches `system.query_log` is the
-- `FilledJoin` and the read of the left side alone.

DROP TABLE IF EXISTS t_05627_fact;
DROP TABLE IF EXISTS t_05627_dim;

CREATE TABLE t_05627_fact (k UInt64, x UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_05627_dim (k UInt64, v UInt64) ENGINE = Join(ALL, LEFT, k);

INSERT INTO t_05627_fact SELECT number, number * 10 FROM numbers(1000);
INSERT INTO t_05627_dim SELECT number, number * 100 FROM numbers(500);

SET log_query_plans = 1;
SELECT count(), sum(t_05627_dim.v)
FROM t_05627_fact LEFT JOIN t_05627_dim ON t_05627_fact.k = t_05627_dim.k
SETTINGS log_comment = '05627_lookup_join' FORMAT Null;
SET log_query_plans = 0;

SYSTEM FLUSH LOGS query_log;

-- The join and the read of the left side are captured. The right side is the prepared table the
-- join holds rather than a step, so no second read appears.
SELECT
    'lookup_join',
    countIf(JSONExtractString(node, 'NodeType') = 'FilledJoin'),
    countIf(JSONExtractString(node, 'NodeType') = 'ReadFromMergeTree'),
    countIf(JSONExtractString(node, 'NodeType') = 'JoinStepLogicalLookup')
FROM
(
    WITH one_plan AS
    (
        SELECT toJSONString(query_plan) AS plan
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND log_comment = '05627_lookup_join'
          AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    )
    SELECT arrayJoin(JSONExtractArrayRaw(plan, 'Nodes')) AS node FROM one_plan
);

-- The join reports what it matched, which is what makes the step worth logging.
SELECT
    'matched',
    JSONExtractUInt(node, 'Statistics', 'IO', 'OutputRows') > 0
FROM
(
    WITH one_plan AS
    (
        SELECT toJSONString(query_plan) AS plan
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND log_comment = '05627_lookup_join'
          AND type = 'QueryFinish'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
    )
    SELECT arrayJoin(JSONExtractArrayRaw(plan, 'Nodes')) AS node FROM one_plan
)
WHERE JSONExtractString(node, 'NodeType') = 'FilledJoin';

DROP TABLE t_05627_fact;
DROP TABLE t_05627_dim;
