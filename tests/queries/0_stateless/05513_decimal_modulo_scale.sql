-- https://github.com/ClickHouse/ClickHouse/issues/73093
-- `modulo` and `positiveModulo` must bring both operands to the scale of the result before dividing.

SET decimal_check_overflow = 1;

SELECT '-- issue';
SELECT toDecimal32(5, 2) % 2, toDecimal64(5, 2) % 3, toDecimal256(5, 18) % 2, 2 % toDecimal32(5, 2);
SELECT toDecimal32(5.5, 1) % toDecimal32(2, 0), toDecimal32(5, 2) % toDecimal32(2, 2);

SELECT '-- Decimal % Int';
SELECT toDecimal32(7.25, 2) % 2 AS x, toTypeName(x), materialize(toDecimal32(7.25, 2)) % 2, toDecimal32(7.25, 2) % materialize(2), materialize(toDecimal32(7.25, 2)) % materialize(2);
SELECT toDecimal64(7.25, 4) % 3 AS x, toTypeName(x), materialize(toDecimal64(7.25, 4)) % 3, toDecimal64(7.25, 4) % materialize(3), materialize(toDecimal64(7.25, 4)) % materialize(3);
SELECT toDecimal128(7.25, 10) % toInt64(2) AS x, toTypeName(x), materialize(toDecimal128(7.25, 10)) % toInt64(2), toDecimal128(7.25, 10) % materialize(toInt64(2)), materialize(toDecimal128(7.25, 10)) % materialize(toInt64(2));
SELECT toDecimal256(7.25, 30) % toUInt8(3) AS x, toTypeName(x), materialize(toDecimal256(7.25, 30)) % toUInt8(3), toDecimal256(7.25, 30) % materialize(toUInt8(3)), materialize(toDecimal256(7.25, 30)) % materialize(toUInt8(3));

SELECT '-- Int % Decimal';
SELECT 7 % toDecimal32(2.5, 1) AS x, toTypeName(x), materialize(7) % toDecimal32(2.5, 1), 7 % materialize(toDecimal32(2.5, 1)), materialize(7) % materialize(toDecimal32(2.5, 1));
SELECT 7 % toDecimal64(2.5, 6) AS x, toTypeName(x), materialize(7) % toDecimal64(2.5, 6), 7 % materialize(toDecimal64(2.5, 6)), materialize(7) % materialize(toDecimal64(2.5, 6));
SELECT 7 % toDecimal128(2.5, 20) AS x, toTypeName(x), materialize(7) % toDecimal128(2.5, 20), 7 % materialize(toDecimal128(2.5, 20)), materialize(7) % materialize(toDecimal128(2.5, 20));
SELECT 7 % toDecimal256(2.5, 40) AS x, toTypeName(x), materialize(7) % toDecimal256(2.5, 40), 7 % materialize(toDecimal256(2.5, 40)), materialize(7) % materialize(toDecimal256(2.5, 40));

SELECT '-- Decimal % Decimal with different scales';
SELECT toDecimal64(10.125, 3) % toDecimal32(1.5, 1) AS x, toTypeName(x), materialize(toDecimal64(10.125, 3)) % toDecimal32(1.5, 1), toDecimal64(10.125, 3) % materialize(toDecimal32(1.5, 1)), materialize(toDecimal64(10.125, 3)) % materialize(toDecimal32(1.5, 1));
SELECT toDecimal32(5, 2) % toDecimal128(3, 1) AS x, toTypeName(x), materialize(toDecimal32(5, 2)) % toDecimal128(3, 1), toDecimal32(5, 2) % materialize(toDecimal128(3, 1)), materialize(toDecimal32(5, 2)) % materialize(toDecimal128(3, 1));
SELECT toDecimal256(100.5, 20) % toDecimal64(7.25, 2) AS x, toTypeName(x), materialize(toDecimal256(100.5, 20)) % toDecimal64(7.25, 2), toDecimal256(100.5, 20) % materialize(toDecimal64(7.25, 2)), materialize(toDecimal256(100.5, 20)) % materialize(toDecimal64(7.25, 2));

SELECT '-- negative operands take the sign of the dividend';
SELECT toDecimal32(-7.25, 2) % 2, toDecimal32(7.25, 2) % -2, toDecimal32(-7.25, 2) % -2, -7 % toDecimal32(2.5, 1), toDecimal32(-5.5, 1) % toDecimal32(2, 0), materialize(toDecimal64(-10.125, 3)) % materialize(toDecimal32(-1.5, 1));

SELECT '-- positiveModulo';
SELECT positiveModulo(toDecimal32(-5, 2), 3) AS x, toTypeName(x), positiveModulo(materialize(toDecimal32(-5, 2)), 3), positiveModulo(toDecimal32(-5, 2), materialize(3)), positiveModulo(materialize(toDecimal32(-5, 2)), materialize(3));
SELECT pmod(toDecimal32(-5, 2), toDecimal32(3, 1)), pmod(toDecimal64(-7.25, 2), 2), pmod(-7, toDecimal128(2.5, 1)), pmod(toDecimal256(-7.25, 30), -2), pmod(materialize(toDecimal64(7.25, 4)), materialize(toDecimal32(-2, 0)));
SELECT mod(toDecimal32(5, 2), 2), positive_modulo(toDecimal32(-5, 2), 3);

SELECT '-- Nullable divisor';
SELECT materialize(toDecimal32(7.25, 2)) % x, toDecimal32(7.25, 2) % x, positiveModulo(toDecimal32(-7.25, 2), x) FROM (SELECT arrayJoin([2, NULL, 3]::Array(Nullable(UInt8))) AS x);
SELECT materialize(toDecimal32(7.25, 2)) % CAST(NULL, 'Nullable(UInt8)'), materialize(toDecimal32(7.25, 2)) % CAST(3, 'Nullable(UInt8)');

SELECT '-- Decimal % Float is computed in Float64';
SELECT toDecimal32(5.5, 1) % 2.0 AS x, toTypeName(x);

SELECT '-- division by zero';
SELECT toDecimal32(5, 2) % 0; -- { serverError ILLEGAL_DIVISION }
SELECT materialize(toDecimal32(5, 2)) % toDecimal32(0, 1); -- { serverError ILLEGAL_DIVISION }
SELECT positiveModulo(materialize(toDecimal32(5, 2)), materialize(0)); -- { serverError ILLEGAL_DIVISION }

SELECT '-- the rescaled operand must fit the result type';
SELECT toDecimal32(5, 2) % 3000000;
SELECT toDecimal32(5, 2) % 30000000; -- { serverError DECIMAL_OVERFLOW }
SELECT materialize(toDecimal32(5, 2)) % materialize(30000000); -- { serverError DECIMAL_OVERFLOW }
SELECT 30000000 % toDecimal32(5, 2); -- { serverError DECIMAL_OVERFLOW }
SELECT materialize(30000000) % materialize(toDecimal32(5, 2)); -- { serverError DECIMAL_OVERFLOW }
SELECT toDecimal32(1, 2) % toDecimal32(99999999, 0); -- { serverError DECIMAL_OVERFLOW }
SELECT positiveModulo(materialize(toDecimal32(5, 2)), 30000000); -- { serverError DECIMAL_OVERFLOW }

SELECT '-- the other modulo functions do not accept Decimal';
SELECT moduloOrZero(toDecimal32(5, 2), 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT moduloOrNull(toDecimal32(5, 2), 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT positiveModuloOrNull(toDecimal32(-5, 2), 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT moduloLegacy(toDecimal32(5, 2), 2); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
