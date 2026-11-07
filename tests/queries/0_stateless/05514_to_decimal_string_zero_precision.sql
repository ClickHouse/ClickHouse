-- `toDecimalString(x, 0)` returns exactly zero fractional digits, so a `Decimal` with a nonzero scale has no decimal point.

SELECT 'constant precision';
SELECT toDecimalString(toDecimal32('1.2', 1), 0), toDecimalString(toDecimal32('1.5', 1), 0), toDecimalString(toDecimal32('-1.2', 1), 0), toDecimalString(toDecimal32('-1.5', 1), 0);
SELECT toDecimalString(toDecimal64('1.2', 1), 0), toDecimalString(toDecimal64('1.5', 1), 0), toDecimalString(toDecimal64('-1.2', 1), 0), toDecimalString(toDecimal64('-1.5', 1), 0);
SELECT toDecimalString(toDecimal128('1.2', 1), 0), toDecimalString(toDecimal128('1.5', 1), 0), toDecimalString(toDecimal128('-1.2', 1), 0), toDecimalString(toDecimal128('-1.5', 1), 0);
SELECT toDecimalString(toDecimal256('1.2', 1), 0), toDecimalString(toDecimal256('1.5', 1), 0), toDecimalString(toDecimal256('-1.2', 1), 0), toDecimalString(toDecimal256('-1.5', 1), 0);

SELECT 'a column of values';
SELECT x, toDecimalString(x, 0) FROM (SELECT arrayJoin([toDecimal64('1.2', 2), toDecimal64('-1.5', 2), toDecimal64('9.95', 2), toDecimal64('3', 2)]) AS x);

SELECT 'a carry into the whole part';
SELECT toDecimalString(toDecimal32('9.5', 1), 0), toDecimalString(toDecimal32('-9.5', 1), 0);
SELECT toDecimalString(toDecimal64('99.99', 2), 0), toDecimalString(toDecimal128('-0.5', 3), 0), toDecimalString(toDecimal256('999.9', 1), 0);

SELECT 'a zero whole part';
SELECT toDecimalString(toDecimal64('0.4', 1), 0), toDecimalString(toDecimal64('0.0', 1), 0), toDecimalString(toDecimal256('0.' || repeat('9', 76), 76), 0);

SELECT 'scale 0, integers and floats';
SELECT toDecimalString(toDecimal32(5, 0), 0), toDecimalString(toDecimal256(-5, 0), 0), toDecimalString(5, 0), toDecimalString(1.5, 0), toDecimalString(-1.2, 0);
