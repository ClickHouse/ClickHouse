-- addDate/subDate/addXxx/subtractXxx and `String +/- INTERVAL` parse a String first argument. The value behind a NULL must never reach the parser.
-- The placeholder under a NULL is the empty string for a plain Nullable(String), and also for the result of a function
-- that is not executed on NULL rows (e.g. formatDateTime), which used to be a parseable string by luck.

SELECT 'plain Nullable(String)';
SELECT addDate(x, toIntervalSecond(0)) IS NULL FROM (SELECT arrayJoin([NULL, '2026-01-01 10:00:00'])::Nullable(String) AS x) ORDER BY 1;
SELECT subDate(x, toIntervalSecond(0)) IS NULL FROM (SELECT arrayJoin([NULL, '2026-01-01 10:00:00'])::Nullable(String) AS x) ORDER BY 1;
SELECT addSeconds(x, 0) IS NULL FROM (SELECT arrayJoin([NULL, '2026-01-01 10:00:00'])::Nullable(String) AS x) ORDER BY 1;
SELECT subtractDays(x, 1) IS NULL FROM (SELECT arrayJoin([NULL, '2026-01-01 10:00:00'])::Nullable(String) AS x) ORDER BY 1;

SELECT 'LowCardinality(Nullable(String))';
SELECT addDate(x, toIntervalSecond(0)) IS NULL FROM (SELECT arrayJoin([NULL, '2026-01-01 10:00:00'])::LowCardinality(Nullable(String)) AS x) ORDER BY 1;
SELECT addSeconds(x, 0) IS NULL FROM (SELECT arrayJoin([NULL, '2026-01-01 10:00:00'])::LowCardinality(Nullable(String)) AS x) ORDER BY 1;

SELECT 'formatDateTime over Nullable(DateTime64) with NULL rows';
SELECT count(e), min(e) = max(e) FROM
(
    SELECT addDate(formatDateTime(c1, '%Y-%m-%d 00:00:00'), toIntervalSecond(0)) AS e
    FROM (SELECT parseDateTime64BestEffortOrNull(arrayJoin(['2026-01-01 10:00:00', '']), 3, 'UTC') AS c1)
);
SELECT count(e), min(e) = max(e) FROM
(
    SELECT addDate(formatDateTime(c1, '%Y-%m-%d 00:00:00'), toIntervalSecond(0)) AS e
    FROM (SELECT parseDateTime64BestEffortOrNull(arrayJoin(['2026-01-01 10:00:00', '']), 3, 'UTC') AS c1)
) SETTINGS compatibility = '24.12';
SELECT count(e), min(e) = max(e) FROM
(
    SELECT addSeconds(formatDateTime(c1, '%Y-%m-%d 00:00:00'), 0) AS e
    FROM (SELECT parseDateTime64BestEffortOrNull(arrayJoin(['2026-01-01 10:00:00', '']), 3, 'UTC') AS c1)
);

SELECT 'plus/minus operators and date_add';
SELECT x + INTERVAL 0 SECOND IS NULL FROM (SELECT arrayJoin([NULL, '2026-01-01 10:00:00'])::Nullable(String) AS x) ORDER BY 1;
SELECT x - INTERVAL 1 DAY IS NULL FROM (SELECT arrayJoin([NULL, '2026-01-01 10:00:00'])::Nullable(String) AS x) ORDER BY 1;
SELECT INTERVAL 1 DAY + x IS NULL FROM (SELECT arrayJoin([NULL, '2026-01-01 10:00:00'])::Nullable(String) AS x) ORDER BY 1;
SELECT x + INTERVAL 0 SECOND IS NULL FROM (SELECT arrayJoin([NULL, '2026-01-01 10:00:00'])::LowCardinality(Nullable(String)) AS x) ORDER BY 1;
SELECT count(e), min(e) = max(e) FROM
(
    SELECT formatDateTime(c1, '%Y-%m-%d 00:00:00') + INTERVAL 0 SECOND AS e
    FROM (SELECT parseDateTime64BestEffortOrNull(arrayJoin(['2026-01-01 10:00:00', '']), 3, 'UTC') AS c1)
);
SELECT count(e), min(e) = max(e) FROM
(
    SELECT formatDateTime(c1, '%Y-%m-%d 00:00:00') - INTERVAL 1 DAY AS e
    FROM (SELECT parseDateTime64BestEffortOrNull(arrayJoin(['2026-01-01 10:00:00', '']), 3, 'UTC') AS c1)
);
SELECT count(e), min(e) = max(e) FROM
(
    SELECT date_add(DAY, 1, formatDateTime(c1, '%Y-%m-%d 00:00:00')) AS e
    FROM (SELECT parseDateTime64BestEffortOrNull(arrayJoin(['2026-01-01 10:00:00', '']), 3, 'UTC') AS c1)
);

SELECT 'date arguments keep working with NULL rows';
SELECT addDays(x, 1) IS NULL FROM (SELECT arrayJoin([NULL, toDate('2026-01-01')])::Nullable(Date) AS x) ORDER BY 1;
SELECT addDate(x, toIntervalDay(1)) IS NULL FROM (SELECT arrayJoin([NULL, toDate('2026-01-01')])::Nullable(Date) AS x) ORDER BY 1;

SELECT 'no NULL in the input';
SELECT addDate(x, toIntervalSecond(0)) IS NULL FROM (SELECT materialize('2026-01-01 10:00:00')::Nullable(String) AS x);
SELECT addSeconds(x, 0) IS NULL FROM (SELECT materialize('2026-01-01 10:00:00')::Nullable(String) AS x);
SELECT x + INTERVAL 0 SECOND IS NULL FROM (SELECT materialize('2026-01-01 10:00:00')::Nullable(String) AS x);
