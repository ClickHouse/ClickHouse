-- `hop`, `hopStart` and `hopEnd` are monotonic in the time, so a table sorted by the time can be read in the order of them.

DROP TABLE IF EXISTS hop_monotonicity;

CREATE TABLE hop_monotonicity (t DateTime('UTC'), x UInt64) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1000;
INSERT INTO hop_monotonicity SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + number * 60, number FROM numbers(100000);

SELECT '--- read in order';
SELECT count() FROM (
    EXPLAIN actions = 1 SELECT hopStart(t, INTERVAL 1 HOUR, INTERVAL 3 HOUR, 'UTC') AS w, x FROM hop_monotonicity ORDER BY w LIMIT 3
    SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1)
WHERE explain LIKE '%Read type: InOrder%';

SELECT count() FROM (
    EXPLAIN actions = 1 SELECT hopEnd(t, INTERVAL 1 MONTH, INTERVAL 2 MONTH, 'UTC') AS w, x FROM hop_monotonicity ORDER BY w DESC LIMIT 3
    SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1)
WHERE explain LIKE '%Read type: InReverseOrder%';

SELECT count() FROM (
    EXPLAIN actions = 1 SELECT hop(t, INTERVAL 1 DAY, INTERVAL 2 DAY, 'UTC') AS w, count() FROM hop_monotonicity GROUP BY w
    SETTINGS optimize_aggregation_in_order = 1, query_plan_aggregation_in_order = 1)
WHERE explain LIKE '%Read type: InOrder%';

SELECT '--- results';
SELECT hopStart(t, INTERVAL 1 HOUR, INTERVAL 3 HOUR, 'UTC') AS w, x FROM hop_monotonicity ORDER BY w, x LIMIT 3
SETTINGS optimize_read_in_order = 1, max_threads = 4;

SELECT hopEnd(t, INTERVAL 1 MONTH, INTERVAL 2 MONTH, 'UTC') AS w, x FROM hop_monotonicity ORDER BY w DESC, x DESC LIMIT 3
SETTINGS optimize_read_in_order = 1, max_threads = 4;

SELECT w, count() FROM (SELECT hop(t, INTERVAL 1 WEEK, INTERVAL 2 WEEK, 'UTC') AS w FROM hop_monotonicity) GROUP BY w ORDER BY w
SETTINGS optimize_aggregation_in_order = 1, max_threads = 4;

DROP TABLE hop_monotonicity;

SELECT '--- the window bounds do not wrap around';
-- A window spanning more than the range of `DateTime` or `Date` used to wrap around to a short window.
SELECT hop(toDateTime('2026-01-01 00:00:00', 'UTC'), INTERVAL 1 SECOND, INTERVAL 4294967297 SECOND, 'UTC'); -- { serverError BAD_ARGUMENTS }
SELECT hop(toDateTime('2026-01-01 00:00:00', 'UTC'), INTERVAL 1 WEEK, INTERVAL 9363 WEEK, 'UTC'); -- { serverError BAD_ARGUMENTS }
SELECT hop(toDateTime('2026-01-01 00:00:00', 'UTC'), INTERVAL 1 WEEK, INTERVAL 9362 WEEK, 'UTC'); -- { serverError BAD_ARGUMENTS }
SELECT hop(toDateTime('2026-01-01 00:00:00', 'UTC'), INTERVAL 1 WEEK, INTERVAL 2900 WEEK, 'UTC');
-- The number of months in a huge window of quarters must not wrap around.
SELECT hop(toDateTime('2026-01-01 00:00:00', 'UTC'), INTERVAL 1 QUARTER, toIntervalQuarter(toInt64(6148914691236517205)), 'UTC'); -- { serverError BAD_ARGUMENTS }
SELECT hop(toDateTime('2026-01-01 00:00:00', 'UTC'), INTERVAL 1 QUARTER, toIntervalQuarter(toInt64(6148914691236517206)), 'UTC'); -- { serverError BAD_ARGUMENTS }
SELECT hop(toDateTime('2026-01-01 00:00:00', 'UTC'), INTERVAL 1 QUARTER, INTERVAL 2 QUARTER, 'UTC');
