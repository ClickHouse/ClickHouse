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
