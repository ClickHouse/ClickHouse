-- `formatReadableTimeDelta` must not print "0 seconds" when a smaller unit is non-zero.

SELECT formatReadableTimeDelta(0.5, 'seconds', 'milliseconds');
SELECT formatReadableTimeDelta(0.0000001, 'years', 'nanoseconds');
SELECT formatReadableTimeDelta(materialize(0.000123), 'hours', 'microseconds');
SELECT formatReadableTimeDelta(-0.25, 'seconds', 'milliseconds');
SELECT formatReadableTimeDelta(toIntervalMillisecond(250), 'hours', 'milliseconds');
SELECT formatReadableTimeDelta(0.5005, 'seconds', 'microseconds');

-- "0 seconds" is still printed when it is the only unit.
SELECT formatReadableTimeDelta(0, 'seconds', 'milliseconds');
SELECT formatReadableTimeDelta(0.0004, 'seconds', 'milliseconds');
SELECT formatReadableTimeDelta(0, 'hours', 'microseconds');
SELECT formatReadableTimeDelta(60.5, 'minutes', 'milliseconds');
SELECT formatReadableTimeDelta(1.5, 'seconds', 'milliseconds');
