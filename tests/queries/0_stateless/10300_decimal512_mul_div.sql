-- Decimal512 multiply/divide regression: ensure truncation to scale 0 no longer crashes
SELECT multiplyDecimal(toDecimal512('0.0001', 4), toDecimal512('2', 0), 0);
SELECT divideDecimal(toDecimal512('0.0001', 4), toDecimal512('2', 0), 0);
