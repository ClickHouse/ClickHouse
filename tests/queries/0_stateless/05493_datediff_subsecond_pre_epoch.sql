-- `dateDiff` and `age` with a subsecond unit coarser than the `DateTime64` scale (for example 'millisecond'
-- on `DateTime64(6)`) used to divide the value with truncation towards zero. Before the epoch that moved a
-- value with a leftover fraction of the unit to the next unit, so the result was off by one.

SELECT 'between the epoch and one tick before it (was 0)';
SELECT dateDiff('millisecond', toDateTime64('1969-12-31 23:59:59.999999', 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC'));
SELECT dateDiff('millisecond', toDateTime64('1969-12-31 23:59:59.9999', 4, 'UTC'), toDateTime64('1970-01-01 00:00:00', 4, 'UTC'));
SELECT dateDiff('millisecond', toDateTime64('1969-12-31 23:59:59.999999999', 9, 'UTC'), toDateTime64('1970-01-01 00:00:00', 9, 'UTC'));
SELECT dateDiff('microsecond', toDateTime64('1969-12-31 23:59:59.999999999', 9, 'UTC'), toDateTime64('1970-01-01 00:00:00', 9, 'UTC'));
SELECT dateDiff('millisecond', toDateTime64('1970-01-01 00:00:00', 6, 'UTC'), toDateTime64('1969-12-31 23:59:59.999999', 6, 'UTC'));

SELECT 'both arguments before the epoch (was 0)';
SELECT dateDiff('millisecond', toDateTime64('1969-12-31 23:59:59.998999', 6, 'UTC'), toDateTime64('1969-12-31 23:59:59.999000', 6, 'UTC'));
SELECT dateDiff('millisecond', toDateTime64('1969-01-01 00:00:00.000001', 6, 'UTC'), toDateTime64('1969-01-01 00:00:00.001000', 6, 'UTC'));

SELECT 'age across the epoch (was 0)';
SELECT age('millisecond', toDateTime64('1969-12-31 23:59:59.998500', 6, 'UTC'), toDateTime64('1970-01-01 00:00:00.000400', 6, 'UTC'));
SELECT age('millisecond', toDateTime64('1970-01-01 00:00:00.000400', 6, 'UTC'), toDateTime64('1969-12-31 23:59:59.998500', 6, 'UTC'));
SELECT age('microsecond', toDateTime64('1969-12-31 23:59:59.999998500', 9, 'UTC'), toDateTime64('1970-01-01 00:00:00.000000400', 9, 'UTC'));

SELECT 'control: unit equal to the scale, after the epoch, whole seconds';
SELECT dateDiff('millisecond', toDateTime64('1969-12-31 23:59:59.999', 3, 'UTC'), toDateTime64('1970-01-01 00:00:00', 3, 'UTC'));
SELECT dateDiff('millisecond', toDateTime64('2024-01-01 00:00:00.000999', 6, 'UTC'), toDateTime64('2024-01-01 00:00:00.001000', 6, 'UTC'));
SELECT dateDiff('second', toDateTime64('1969-12-31 23:59:59.999999', 6, 'UTC'), toDateTime64('1970-01-01 00:00:00', 6, 'UTC'));

-- `age` compares the fractional parts of its arguments, which used to be in units of each argument's own scale.
SELECT 'age with different scales, 300 microseconds apart';
SELECT age('millisecond', toDateTime64('1969-12-31 23:59:59.9999', 4, 'UTC'), toDateTime64('1970-01-01 00:00:00.000200', 6, 'UTC'));
SELECT age('millisecond', toDateTime64('1970-01-01 00:00:00.000200', 6, 'UTC'), toDateTime64('1969-12-31 23:59:59.9999', 4, 'UTC'));
SELECT age('millisecond', toDateTime64('2024-01-01 00:00:00.0009', 4, 'UTC'), toDateTime64('2024-01-01 00:00:00.001200', 6, 'UTC'));
SELECT age('millisecond', toDateTime64('2024-01-01 00:00:00.001200', 6, 'UTC'), toDateTime64('2024-01-01 00:00:00.0009', 4, 'UTC'));

SELECT 'age with different scales, other units (was 1 and -1)';
SELECT age('microsecond', toDateTime64('2024-01-01 00:00:00.0000009', 7, 'UTC'), toDateTime64('2024-01-01 00:00:00.000001200', 9, 'UTC'));
SELECT age('microsecond', toDateTime64('2024-01-01 00:00:00.000001200', 9, 'UTC'), toDateTime64('2024-01-01 00:00:00.0000009', 7, 'UTC'));
SELECT age('second', toDateTime64('2024-01-01 00:00:00.50', 2, 'UTC'), toDateTime64('2024-01-01 00:00:01.100', 3, 'UTC'));
SELECT age('second', toDateTime64('2024-01-01 00:00:01.100', 3, 'UTC'), toDateTime64('2024-01-01 00:00:00.50', 2, 'UTC'));
SELECT age('day', toDateTime64('2024-01-01 00:00:00.9', 1, 'UTC'), toDateTime64('2024-01-02 00:00:00.100', 3, 'UTC'));
SELECT age('day', toDateTime64('2024-01-02 00:00:00.100', 3, 'UTC'), toDateTime64('2024-01-01 00:00:00.9', 1, 'UTC'));

-- `dateDiff` counts unit boundaries, so it is the difference of the floored values. `age` counts whole
-- units, so it is the difference truncated towards zero.
SELECT 'sweep around the epoch, DateTime64(6), rows that disagree';
SELECT
    countIf(dateDiff('millisecond', a, b) != intDiv(y - positiveModulo(y, 1000), 1000) - intDiv(x - positiveModulo(x, 1000), 1000)),
    countIf(age('millisecond', a, b) != intDiv(y - x, 1000))
FROM
(
    SELECT x, y, fromUnixTimestamp64Micro(x, 'UTC') AS a, fromUnixTimestamp64Micro(y, 'UTC') AS b
    FROM (SELECT toInt64(number) - 1500 AS x FROM numbers(2500)) AS xs CROSS JOIN (SELECT toInt64(number) * 37 - 2000 AS y FROM numbers(100)) AS ys
);

SELECT 'sweep around the epoch, DateTime64(9), rows that disagree';
SELECT
    countIf(dateDiff('microsecond', a, b) != intDiv(y - positiveModulo(y, 1000), 1000) - intDiv(x - positiveModulo(x, 1000), 1000)),
    countIf(age('microsecond', a, b) != intDiv(y - x, 1000)),
    countIf(dateDiff('millisecond', a, b) != intDiv(y - positiveModulo(y, 1000000), 1000000) - intDiv(x - positiveModulo(x, 1000000), 1000000)),
    countIf(age('millisecond', a, b) != intDiv(y - x, 1000000))
FROM
(
    SELECT x, y, fromUnixTimestamp64Nano(x, 'UTC') AS a, fromUnixTimestamp64Nano(y, 'UTC') AS b
    FROM (SELECT toInt64(number) * 1009 - 1500000 AS x FROM numbers(2500)) AS xs CROSS JOIN (SELECT toInt64(number) * 37003 - 2000000 AS y FROM numbers(100)) AS ys
);

SELECT 'sweep around the epoch, DateTime64(4) against DateTime64(6), rows that disagree';
SELECT
    countIf(dateDiff('millisecond', a, b) != intDiv(y - positiveModulo(y, 1000), 1000) - intDiv(x - positiveModulo(x, 1000), 1000)),
    countIf(age('millisecond', a, b) != intDiv(y - x, 1000)),
    countIf(age('millisecond', b, a) != intDiv(x - y, 1000)),
    countIf(age('second', a, b) != intDiv(y - x, 1000000)),
    countIf(age('second', b, a) != intDiv(x - y, 1000000))
FROM
(
    SELECT x, y, CAST(fromUnixTimestamp64Micro(x, 'UTC') AS DateTime64(4, 'UTC')) AS a, fromUnixTimestamp64Micro(y, 'UTC') AS b
    FROM (SELECT toInt64(number) * 1100 - 1500000 AS x FROM numbers(2500)) AS xs CROSS JOIN (SELECT toInt64(number) * 37003 - 2000000 AS y FROM numbers(100)) AS ys
);
