-- Random settings limits: short_circuit_function_evaluation_for_nulls_threshold=(1, None)
-- https://github.com/ClickHouse/ClickHouse/issues/130381
-- The rows behind a NULL of a `Nullable(String)` hold an empty string, which is not a valid date:
-- the functions that add or subtract an interval must not parse it.

SELECT 'the query from the issue';
SELECT min(addDate(formatDateTime(c1, '%Y-%m-%d 00:00:00'), toIntervalSecond(0))) AS lo, max(addDate(formatDateTime(c1, '%Y-%m-%d 00:00:00'), toIntervalSecond(0))) AS hi
FROM (SELECT parseDateTime64BestEffortOrNull(arrayJoin(['2026-01-01 10:00:00', '']), 3, 'UTC') AS c1)
SETTINGS compatibility = '24.12';

SELECT 'over formatDateTime';
SELECT
    max(formatDateTime(c1, '%Y-%m-%d 00:00:00') + INTERVAL 0 SECOND),
    max(formatDateTime(c1, '%Y-%m-%d 00:00:00') - INTERVAL 1 DAY),
    max(date_add(DAY, 1, formatDateTime(c1, '%Y-%m-%d 00:00:00'))),
    max(addSeconds(formatDateTime(c1, '%Y-%m-%d 00:00:00'), 0)),
    max(subDate(formatDateTime(c1, '%Y-%m-%d 00:00:00'), INTERVAL 1 DAY))
FROM (SELECT parseDateTime64BestEffortOrNull(arrayJoin(['2026-01-01 10:00:00', '']), 3, 'UTC') AS c1);

SELECT 'Nullable(String)';
SELECT x + INTERVAL 1 DAY, INTERVAL 1 DAY + x, x - INTERVAL 1 DAY, addDate(x, INTERVAL 1 DAY), subDate(x, INTERVAL 1 DAY), addDays(x, 1), subtractHours(x, 1)
FROM (SELECT arrayJoin([NULL, '2026-01-01 10:00:00'])::Nullable(String) AS x)
ORDER BY x;

SELECT 'LowCardinality(Nullable(String))';
SELECT x + INTERVAL 1 DAY, x - INTERVAL 1 DAY, addDate(x, INTERVAL 1 DAY), addDays(x, 1)
FROM (SELECT arrayJoin([NULL, '2026-01-01 10:00:00'])::LowCardinality(Nullable(String)) AS x)
ORDER BY x;

SELECT 'a value that is not a date still throws';
SELECT x + INTERVAL 1 DAY FROM (SELECT arrayJoin([NULL, 'garbage'])::Nullable(String) AS x); -- { serverError CANNOT_PARSE_DATETIME }
SELECT addDate(x, INTERVAL 1 DAY) FROM (SELECT arrayJoin([NULL, 'garbage'])::Nullable(String) AS x); -- { serverError CANNOT_PARSE_DATETIME }
SELECT addDays(x, 1) FROM (SELECT arrayJoin([NULL, 'garbage'])::Nullable(String) AS x); -- { serverError CANNOT_PARSE_DATETIME }

SELECT 'a Nullable(Date) keeps the numeric fast path';
SELECT d + INTERVAL 1 DAY, addDate(d, INTERVAL 1 DAY), addDays(d, 1)
FROM (SELECT if(number = 0, NULL, toDate(number)) AS d FROM numbers(1000))
FORMAT Null
SETTINGS log_comment = 'nullable_date_interval_fast_path';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['DefaultImplementationForNullsRows']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = 'nullable_date_interval_fast_path';
