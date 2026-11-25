SET session_timezone = 'UTC';

-- One constant argument.
SELECT timestamp(s, '01:00:00') FROM (SELECT arrayJoin(['2025-01-01', '2025-01-02']) AS s) ORDER BY ALL;
SELECT timestamp('2025-01-01', t) FROM (SELECT arrayJoin(['01:00:00', '02:00:00']) AS t) ORDER BY ALL;
SELECT timestamp(toFixedString('2025-01-01', 10), t) FROM (SELECT arrayJoin(['01:00:00']) AS t);
SELECT timestamp(CAST('2025-01-01' AS Nullable(String)), t) FROM (SELECT arrayJoin(['01:00:00']) AS t);
SELECT timestamp(toLowCardinality(s), '01:00:00') FROM (SELECT arrayJoin(['2025-01-01']) AS s);
SELECT timestamp(d, t) FROM values('d String, t String', ('2023-01-01', '12:00:00'), ('2023-01-01', '13:00:00'), ('2024-01-01', '13:00:00'))
WHERE d = '2023-01-01' ORDER BY ALL;

-- The value behind a `NULL` is not parsed, whatever the `NULL` short-circuit settings.
SELECT timestamp(materialize('2025-01-01'), nt) FROM (SELECT arrayJoin([CAST('01:00:00' AS Nullable(String)), NULL]) AS nt) ORDER BY ALL;
SELECT timestamp(materialize('2025-01-01'), nt) FROM (SELECT arrayJoin([CAST('01:00:00' AS Nullable(String)), NULL]) AS nt) ORDER BY ALL
SETTINGS short_circuit_function_evaluation_for_nulls = 0;
SELECT timestamp(materialize('2025-01-01'), lt) FROM values('lt LowCardinality(Nullable(String))', ('01:00:00'), (NULL)) ORDER BY ALL
SETTINGS short_circuit_function_evaluation_for_nulls = 0;
SELECT timestamp(nfs) FROM (SELECT arrayJoin([CAST('2025-01-01' AS Nullable(FixedString(10))), NULL]) AS nfs) ORDER BY ALL
SETTINGS short_circuit_function_evaluation_for_nulls = 0;
SELECT timestamp(ns, '01:00:00') FROM (SELECT arrayJoin([CAST('2025-01-01' AS Nullable(String)), NULL]) AS ns) ORDER BY ALL;

-- An equality after `timestamp` in the same `WHERE` is filtered first and makes its argument constant.
SET query_plan_merge_filters = 1, optimize_constant_columns_after_filter = 1;
SET short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 1;
SELECT t FROM values('d String, t String', ('2023-01-01', '12:00:00'), ('2023-01-01', '13:00:00'), ('2024-01-01', '13:00:00'))
WHERE timestamp(d, t) > '2000-01-01' AND d = '2023-01-01' ORDER BY ALL;
SELECT d FROM values('d String, t String', ('2023-01-01', '12:00:00'), ('2023-01-01', '13:00:00'), ('2024-01-01', '13:00:00'))
WHERE timestamp(d, t) > '2000-01-01' AND t = '13:00:00' ORDER BY ALL;
