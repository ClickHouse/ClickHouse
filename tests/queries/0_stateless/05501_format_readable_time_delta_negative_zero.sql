-- `formatReadableTimeDelta` must format negative zero (e.g. the result of `round(-0.0001, 2)`)
-- like positive zero with sub-second units, not as a huge number of milliseconds.

SELECT formatReadableTimeDelta(-0., 'seconds', 'milliseconds');
SELECT formatReadableTimeDelta(-0., 'seconds', 'microseconds');
SELECT formatReadableTimeDelta(-0., 'seconds', 'nanoseconds');
SELECT formatReadableTimeDelta(-0., 'years', 'nanoseconds');
SELECT formatReadableTimeDelta(-0., 'milliseconds');
SELECT formatReadableTimeDelta(-0., 'microseconds', 'microseconds');
SELECT formatReadableTimeDelta(materialize(-0.), 'seconds', 'milliseconds');
SELECT formatReadableTimeDelta(toFloat32(-0.), 'seconds', 'milliseconds');
SELECT formatReadableTimeDelta(round(-0.0001, 2), 'seconds', 'milliseconds');

-- Positive zero and the default units, for comparison.
SELECT formatReadableTimeDelta(0., 'seconds', 'milliseconds');
SELECT formatReadableTimeDelta(0., 'seconds', 'microseconds');
SELECT formatReadableTimeDelta(0., 'seconds', 'nanoseconds');
SELECT formatReadableTimeDelta(0., 'milliseconds');
SELECT formatReadableTimeDelta(-0.);
