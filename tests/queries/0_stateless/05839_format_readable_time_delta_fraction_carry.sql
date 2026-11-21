-- `formatReadableTimeDelta` must carry a fraction that rounds up to a whole second into the larger units.

SELECT formatReadableTimeDelta(0.9999999996, 'seconds', 'nanoseconds');
SELECT formatReadableTimeDelta(59.9999999996, 'minutes', 'milliseconds');
SELECT formatReadableTimeDelta(2.3 - 1.3, 'milliseconds');
SELECT formatReadableTimeDelta(0.9999999996, 'microseconds', 'microseconds');
SELECT formatReadableTimeDelta(86399.9999999996, 'days', 'nanoseconds');
SELECT formatReadableTimeDelta(-0.9999999996, 'seconds', 'nanoseconds');
SELECT formatReadableTimeDelta(materialize(0.9999999996), 'seconds', 'nanoseconds');

-- Fractions that do not round up to a whole second, for comparison.
SELECT formatReadableTimeDelta(0.9999999994, 'seconds', 'nanoseconds');
SELECT formatReadableTimeDelta(0.0000000006, 'seconds', 'nanoseconds');
SELECT formatReadableTimeDelta(0.9996, 'seconds', 'milliseconds');
