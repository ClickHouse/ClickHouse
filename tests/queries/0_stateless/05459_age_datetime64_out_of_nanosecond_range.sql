-- age ordered its operands by a nanosecond count that overflows outside [1677-09-21, 2262-04-11]
SET session_timezone = 'UTC';

-- a shorter interval must not report more whole units
SELECT age('second', toDateTime64('3000-01-01 00:00:00.000', 3), toDateTime64('2015-08-18 00:00:00.000', 3)), age('second', toDateTime64('3000-01-01 00:00:00.999', 3), toDateTime64('2015-08-18 00:00:00.000', 3));
SELECT age('minute', toDateTime64('2290-01-01 00:00:00.000', 3), toDateTime64('2015-08-18 00:00:00.000', 3)), age('minute', toDateTime64('2290-01-01 00:00:30.000', 3), toDateTime64('2015-08-18 00:00:00.000', 3));
SELECT age('hour', toDateTime64('2290-01-01 00:00:00.000', 3), toDateTime64('2015-08-18 00:00:00.000', 3)), age('hour', toDateTime64('2290-01-01 00:30:00.000', 3), toDateTime64('2015-08-18 00:00:00.000', 3));
SELECT age('day', toDateTime64('2290-01-01 00:00:00.000', 3), toDateTime64('2015-08-18 00:00:00.000', 3)), age('day', toDateTime64('2290-01-01 12:00:00.000', 3), toDateTime64('2015-08-18 00:00:00.000', 3));
SELECT age('week', toDateTime64('2290-01-07 00:00:00.000', 3), toDateTime64('2015-08-18 00:00:00.000', 3)), age('week', toDateTime64('2290-01-08 00:00:00.000', 3), toDateTime64('2015-08-18 00:00:00.000', 3));
SELECT age('month', toDateTime64('2290-01-18 00:00:00.000', 3), toDateTime64('2015-08-18 00:00:00.000', 3)), age('month', toDateTime64('2290-01-19 00:00:00.000', 3), toDateTime64('2015-08-18 00:00:00.000', 3));
SELECT age('quarter', toDateTime64('2290-02-18 00:00:00.000', 3), toDateTime64('2015-08-18 00:00:00.000', 3)), age('quarter', toDateTime64('2290-02-19 00:00:00.000', 3), toDateTime64('2015-08-18 00:00:00.000', 3));
SELECT age('year', toDateTime64('2290-08-18 00:00:00.000', 3), toDateTime64('2015-08-18 00:00:00.000', 3)), age('year', toDateTime64('2290-08-19 00:00:00.000', 3), toDateTime64('2015-08-18 00:00:00.000', 3));

-- the same intervals in the other direction
SELECT age('second', toDateTime64('2015-08-18 00:00:00.000', 3), toDateTime64('3000-01-01 00:00:00.000', 3)), age('second', toDateTime64('2015-08-18 00:00:00.000', 3), toDateTime64('3000-01-01 00:00:00.999', 3));
SELECT age('day', toDateTime64('2015-08-18 00:00:00.000', 3), toDateTime64('2290-01-01 00:00:00.000', 3)), age('day', toDateTime64('2015-08-18 00:00:00.000', 3), toDateTime64('2290-01-01 12:00:00.000', 3));

-- both operands outside the window
SELECT age('second', toDateTime64('3000-01-01 00:00:00.999', 3), toDateTime64('3000-01-01 00:00:00.000', 3)), age('second', toDateTime64('3000-01-01 00:00:00.000', 3), toDateTime64('3000-01-01 00:00:00.999', 3));
SELECT age('second', toDateTime64('1500-01-01 00:00:00.500', 3), toDateTime64('2015-08-18 00:00:00.000', 3)), age('second', toDateTime64('2015-08-18 00:00:00.000', 3), toDateTime64('1500-01-01 00:00:00.500', 3));

-- a saturated cast carries a maximal fractional part
SELECT age('second', toDateTime64(1e20, 3), toDateTime64('2015-08-18 00:00:00.000', 3));

-- a Date past 2106-02-07 does not fit UInt32 seconds
SELECT dateDiff('second', toDate('2120-01-01'), toDate('2120-01-02'));
SELECT age('day', toDateTime64('2120-01-01 12:00:00.000', 3), toDate('2120-01-02')), age('day', toDate('2120-01-02'), toDateTime64('2120-01-01 12:00:00.000', 3));
