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

SELECT '--- multi-hour hops across a time zone rollback';
-- Multi-hour intervals are counted from the local midnight, so when the clocks go back the window can move backwards:
-- in `Pacific/Chatham` the clocks go from 03:45 back to 02:45, and a three-hour window moves from 03:00 back to 00:00.
-- The functions are not monotonic for such a combination of the interval and the time zone.
DROP TABLE IF EXISTS hop_rollback;

CREATE TABLE hop_rollback (t DateTime('UTC')) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
INSERT INTO hop_rollback SELECT toDateTime('2026-04-04 00:00:00', 'UTC') + number * 60 FROM numbers(2880);

SELECT count() FROM (
    EXPLAIN actions = 1 SELECT t FROM hop_rollback ORDER BY hopStart(t, INTERVAL 3 HOUR, INTERVAL 3 HOUR, 'Pacific/Chatham') LIMIT 1
    SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1)
WHERE explain LIKE '%Read type: InOrder%';

SELECT count() FROM (
    EXPLAIN actions = 1 SELECT t FROM hop_rollback ORDER BY hopEnd(t, INTERVAL 2 HOUR, INTERVAL 4 HOUR, 'Antarctica/Troll') LIMIT 1
    SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1)
WHERE explain LIKE '%Read type: InOrder%';

-- The rollback of a usual daylight saving time does not break the order of three-hour windows, and one-hour hops are always ordered.
SELECT count() FROM (
    EXPLAIN actions = 1 SELECT t FROM hop_rollback ORDER BY hopStart(t, INTERVAL 3 HOUR, INTERVAL 3 HOUR, 'Europe/Berlin') LIMIT 1
    SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1)
WHERE explain LIKE '%Read type: InOrder%';

SELECT count() FROM (
    EXPLAIN actions = 1 SELECT t FROM hop_rollback ORDER BY hopStart(t, INTERVAL 1 HOUR, INTERVAL 3 HOUR, 'Pacific/Chatham') LIMIT 1
    SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1)
WHERE explain LIKE '%Read type: InOrder%';

-- The results are the same with and without the optimizations.
SELECT t FROM hop_rollback WHERE t >= '2026-04-04 13:15:00' ORDER BY hopStart(t, INTERVAL 3 HOUR, INTERVAL 3 HOUR, 'Pacific/Chatham'), t LIMIT 1
SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1, max_threads = 1;
SELECT t FROM hop_rollback WHERE t >= '2026-04-04 13:15:00' ORDER BY hopStart(t, INTERVAL 3 HOUR, INTERVAL 3 HOUR, 'Pacific/Chatham'), t LIMIT 1
SETTINGS optimize_read_in_order = 0, query_plan_read_in_order = 0, max_threads = 1;

SELECT t FROM hop_rollback WHERE t >= '2026-04-04 13:15:00' ORDER BY hopEnd(t, INTERVAL 3 HOUR, INTERVAL 3 HOUR, 'Pacific/Chatham'), t LIMIT 1
SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1, max_threads = 1;
SELECT t FROM hop_rollback WHERE t >= '2026-04-04 13:15:00' ORDER BY hopEnd(t, INTERVAL 3 HOUR, INTERVAL 3 HOUR, 'Pacific/Chatham'), t LIMIT 1
SETTINGS optimize_read_in_order = 0, query_plan_read_in_order = 0, max_threads = 1;

SELECT w, count() FROM (SELECT hop(t, INTERVAL 3 HOUR, INTERVAL 3 HOUR, 'Pacific/Chatham') AS w FROM hop_rollback WHERE t >= '2026-04-04 10:00:00' AND t < '2026-04-04 18:00:00') GROUP BY w ORDER BY w
SETTINGS optimize_aggregation_in_order = 1, query_plan_aggregation_in_order = 1, max_threads = 1;
SELECT w, count() FROM (SELECT hop(t, INTERVAL 3 HOUR, INTERVAL 3 HOUR, 'Pacific/Chatham') AS w FROM hop_rollback WHERE t >= '2026-04-04 10:00:00' AND t < '2026-04-04 18:00:00') GROUP BY w ORDER BY w
SETTINGS optimize_aggregation_in_order = 0, query_plan_aggregation_in_order = 0, max_threads = 1;

DROP TABLE hop_rollback;
