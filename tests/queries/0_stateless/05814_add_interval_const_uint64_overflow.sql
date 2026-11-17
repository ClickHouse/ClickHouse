-- https://github.com/ClickHouse/ClickHouse/issues/132274
-- A constant count that does not fit Int64 must throw for a column date too, not wrap to a negative count.

SELECT addDays(materialize(toDate('2000-01-01')), 18446744073709551613); -- { serverError DECIMAL_OVERFLOW }
SELECT addDays(materialize(toDate32('2000-01-01')), 18446744073709551613); -- { serverError DECIMAL_OVERFLOW }
SELECT addDays(materialize('2000-01-01'), 18446744073709551613); -- { serverError DECIMAL_OVERFLOW }
SELECT subtractDays(materialize(toDate('2000-01-01')), 18446744073709551613); -- { serverError DECIMAL_OVERFLOW }
SELECT addWeeks(materialize(toDate('2000-01-01')), 18446744073709551615); -- { serverError DECIMAL_OVERFLOW }
SELECT addMonths(materialize(toDate('2000-01-01')), 18446744073709551615); -- { serverError DECIMAL_OVERFLOW }
SELECT addYears(materialize(toDate('2000-01-01')), 18446744073709551615); -- { serverError DECIMAL_OVERFLOW }
SELECT addSeconds(materialize(toDateTime('2000-01-01 00:00:00', 'UTC')), 18446744073709551613); -- { serverError DECIMAL_OVERFLOW }
SELECT addDays(materialize(toDateTime64('2000-01-01 00:00:00', 3, 'UTC')), 18446744073709551613); -- { serverError DECIMAL_OVERFLOW }
SELECT date_add(DAY, 18446744073709551613, materialize(toDate('2000-01-01'))); -- { serverError DECIMAL_OVERFLOW }
SELECT toDate('2000-01-01') + number AS d, addDays(d, 18446744073709551613) FROM numbers(2); -- { serverError DECIMAL_OVERFLOW }
SELECT addDays(materialize(toDate('2000-01-01')), 1e30); -- { serverError DECIMAL_OVERFLOW }

SELECT addDays(materialize(toDate('2000-01-01')), toUInt64(3));
SELECT addDays(materialize(toDate('2000-01-01')), -3);
SELECT addSeconds(materialize(toDateTime('2000-01-01 00:00:00', 'UTC')), 1.5);
SELECT subtractDays(materialize('2000-01-01'), toUInt8(1));

SELECT fromModifiedJulianDay(18446744073709551613); -- { serverError CANNOT_FORMAT_DATETIME }
SELECT fromModifiedJulianDay(toInt128('-18446744073709551616')); -- { serverError CANNOT_FORMAT_DATETIME }
SELECT fromModifiedJulianDayOrNull(18446744073709551613);
SELECT fromModifiedJulianDayOrNull(toInt128('-18446744073709551616'));
SELECT fromModifiedJulianDayOrNull(toUInt256('18446744073709608465'));
SELECT fromModifiedJulianDay(toUInt64(58849)), fromModifiedJulianDayOrNull(toInt128(58849));
