-- `formatReadableTimeDelta` with a minimum unit above seconds must not throw `std::length_error`
-- when a unit other than the minimum one is the last non-zero unit after another printed unit.

SELECT formatReadableTimeDelta(365 * 86400 + 30.5 * 86400, 'years', 'hours');
SELECT formatReadableTimeDelta(365 * 86400 + 30.5 * 86400 + 0.5, 'years', 'hours');
SELECT formatReadableTimeDelta(365 * 86400 + 30.5 * 86400, 'years', 'days');
SELECT formatReadableTimeDelta(-(365 * 86400 + 30.5 * 86400), 'years', 'hours');
SELECT formatReadableTimeDelta(materialize(90000.5), 'years', 'minutes');
SELECT formatReadableTimeDelta(365 * 86400, 'months', 'minutes');
SELECT formatReadableTimeDelta(31.5 * 86400, 'days', 'minutes');

-- These did not throw before.
SELECT formatReadableTimeDelta(365 * 86400 + 3600.7, 'years', 'hours');
SELECT formatReadableTimeDelta(365 * 86400 + 30.5 * 86400 + 3630.5, 'years', 'hours');
SELECT formatReadableTimeDelta(365 * 86400 + 0.5, 'years', 'hours');
SELECT formatReadableTimeDelta(90000.5, 'days', 'hours');
SELECT formatReadableTimeDelta(90060.5, 'days', 'minutes');

-- A leftover of one second or more that stays below the minimum unit must still use the last
-- separator " and ", even when the last printed unit is above the minimum unit (previously ", ").
SELECT formatReadableTimeDelta(86400 + 3600 + 30, 'days', 'minutes');
SELECT formatReadableTimeDelta(365 * 86400 + 30.5 * 86400 + 3600 + 30, 'years', 'minutes');
SELECT formatReadableTimeDelta(-(86400 + 3600 + 30), 'days', 'minutes');

-- A value below one minimum unit above seconds prints zero of that unit (previously an empty string or a bare "-").
SELECT x, formatReadableTimeDelta(x, 'days', 'hours') FROM (SELECT arrayJoin([-5400, -1800, 0, 1800, 5400]) AS x) ORDER BY x;
SELECT formatReadableTimeDelta(30, 'minutes', 'minutes');
SELECT formatReadableTimeDelta(-0.5, 'years', 'years');
SELECT formatReadableTimeDelta(materialize(86399.9), 'months', 'days');
SELECT formatReadableTimeDelta(toIntervalMinute(59), 'days', 'hours');
SELECT x, formatReadableTimeDelta(x, 'minutes', 'seconds') FROM (SELECT arrayJoin([-0.5, 0, 0.5]) AS x) ORDER BY x;
