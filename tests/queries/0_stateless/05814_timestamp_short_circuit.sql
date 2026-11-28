-- `timestamp` throws on a value it cannot parse, so a condition in front of it must keep it away from such rows.
SET session_timezone = 'UTC';
SET short_circuit_function_evaluation = 'enable';

-- Short-circuit functions.
SELECT if(s != 'x', timestamp(s), NULL) FROM (SELECT arrayJoin(['x', '2025-01-01']) AS s);
SELECT multiIf(s = 'x', NULL, timestamp(s)) FROM (SELECT arrayJoin(['x', '2025-01-01']) AS s);
SELECT sum(s != 'x' AND timestamp(s) > '2020-01-01') FROM (SELECT arrayJoin(['x', '2025-01-01']) AS s);
SELECT sum(s = 'x' OR timestamp(s) > '2020-01-01') FROM (SELECT arrayJoin(['x', '2025-01-01']) AS s);
SELECT if(t = '', NULL, timestamp(s, t)) FROM (SELECT materialize('2025-01-01') AS s, arrayJoin(['', '01:00:00']) AS t);
SELECT sum(if(s != 'x', toUnixTimestamp64Micro(timestamp(s)), 0)) FROM (SELECT arrayJoin(['x', '2025-01-01']) AS s)
    SETTINGS optimize_rewrite_aggregate_function_with_if = 1;
SELECT sum(if(s != 'x', toUnixTimestamp64Micro(timestamp(s)), 0)) FROM (SELECT arrayJoin(['x', '2025-01-01']) AS s)
    SETTINGS optimize_rewrite_aggregate_function_with_if = 0;

-- Conditions in WHERE and PREWHERE.
DROP TABLE IF EXISTS t_timestamp_guard;
CREATE TABLE t_timestamp_guard (s String, lc LowCardinality(String), ns Nullable(String), payload String)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_timestamp_guard VALUES ('2025-05-16 07:20:20', '2025-05-16 07:20:20', '2025-05-16 07:20:20', 'ok'),
    ('invaliddatetime', 'invaliddatetime', 'invaliddatetime', 'bad');

SELECT payload FROM t_timestamp_guard WHERE s != 'invaliddatetime' AND timestamp(s) > '2020-01-01'
    SETTINGS optimize_move_to_prewhere = 1, enable_multiple_prewhere_read_steps = 1;
SELECT payload FROM t_timestamp_guard WHERE s != 'invaliddatetime' AND timestamp(s) > '2020-01-01'
    SETTINGS optimize_move_to_prewhere = 1, enable_multiple_prewhere_read_steps = 0;
SELECT payload FROM t_timestamp_guard PREWHERE s != 'invaliddatetime' AND timestamp(s) > '2020-01-01';
SELECT payload FROM t_timestamp_guard WHERE lc != 'invaliddatetime' AND timestamp(lc) > '2020-01-01'
    SETTINGS optimize_move_to_prewhere = 1;
SELECT payload FROM t_timestamp_guard WHERE ns != 'invaliddatetime' AND timestamp(ns) > '2020-01-01'
    SETTINGS optimize_move_to_prewhere = 1;

-- Without a guard the value really cannot be parsed.
SELECT timestamp(materialize('invaliddatetime')); -- { serverError CANNOT_PARSE_DATETIME }
SELECT if(s != 'x', timestamp(s), NULL) FROM (SELECT arrayJoin(['x', '2025-01-01']) AS s)
    SETTINGS short_circuit_function_evaluation = 'disable'; -- { serverError CANNOT_PARSE_DATETIME }

DROP TABLE t_timestamp_guard;
