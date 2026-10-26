-- The month and quarter helpers of `DateLUTImpl` step over the month boundaries; at the ends of the LUT
-- (1900-01 and 2299-12) such a step must saturate instead of wrapping around.
SET enable_extended_results_for_datetime_functions = 1;

SELECT toStartOfQuarter(toDate32('1900-01-15')), toStartOfQuarter(toDate32('1900-03-15')), toStartOfQuarter(toDate32('2299-12-31'));
SELECT toStartOfMonth(toDate32('1900-01-01')), toLastDayOfMonth(toDate32('1900-01-15')), toLastDayOfMonth(toDate32('2299-12-15'));
SELECT toStartOfInterval(toDateTime64('1900-01-15 00:00:00', 0, 'UTC'), INTERVAL 1 MONTH), toStartOfInterval(toDateTime64('2299-12-31 00:00:00', 0, 'UTC'), INTERVAL 1 QUARTER);

SELECT count()
FROM (SELECT toDate32('1900-01-01') + number AS d FROM numbers(146000))
WHERE toStartOfQuarter(d) != makeDate32(toYear(d), intDiv(toMonth(d) - 1, 3) * 3 + 1, 1)
    OR toStartOfQuarter(toDateTime64(d, 0, 'UTC')) != makeDate32(toYear(d), intDiv(toMonth(d) - 1, 3) * 3 + 1, 1);
