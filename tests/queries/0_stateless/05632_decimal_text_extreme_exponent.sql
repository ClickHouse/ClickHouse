-- A decimal text exponent close to the `Int32` limits must not wrap around:
-- `1e2147483647` used to pass the "too big" check and read as 0.

SELECT '-- huge positive exponent does not fit';
SELECT toDecimal64OrNull('1e2147483647', 2), toDecimal32OrNull('1e2147483647', 0), toDecimal256OrNull('-1e2147483647', 10);
SELECT toDecimal64('1e2147483647', 2); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM format(CSV, 'd Decimal(10, 2)', '1e2147483647'); -- { serverError ARGUMENT_OUT_OF_BOUND }

SELECT '-- exponent sum out of range';
SELECT toDecimal64OrNull('0.1e-2147483648', 2);
SELECT toDecimal64('0.1e-2147483648', 2); -- { serverError CANNOT_PARSE_NUMBER }

SELECT '-- huge negative exponent rounds to zero';
SELECT toDecimal64OrNull('1e-2147483648', 2), toDecimal64OrNull('1e-2147483647', 2), toDecimal128('-5e-2147483648', 3);

SELECT '-- ordinary exponents';
SELECT toDecimal64('1.5e3', 2), toDecimal64('15e-1', 2), toDecimal64('1e15', 2), toDecimal64OrNull('1e16', 2);
