-- Rounding an integer with a negative N must return the exact result whenever it is representable in the argument type.

SELECT floor(toInt8(-50), -2), ceil(toInt8(50), -2), round(toInt8(-50), -2), roundBankers(toInt8(-70), -2), ceil(toUInt8(180), -2);
SELECT floor(toInt8(-120), -1), ceil(toInt8(120), -1), ceil(toUInt8(250), -1), round(toInt8(-120), -1), roundBankers(toInt8(-125), -1);
SELECT round(toInt32(2000000000), -9), floor(toInt32(-1500000000), -9), ceil(toInt32(1500000000), -9), roundBankers(toInt32(-1700000000), -9), round(toUInt32(4000000000), -9), ceil(toUInt32(3500000000), -9);
SELECT floor(toInt64(-9223372036854775800), -1), ceil(toInt64(9223372036854775800), -1), round(toInt64(9000000000000000000), -18), roundBankers(toInt64(-9223372036854775805), -1), ceil(toUInt64(18446744073709551610), -1), round(toUInt64(18446744073709551614), -1);
SELECT floor(toInt128('-170141183460469231731687303715884105720'), -1), round(toInt128('-170141183460469231731687303715884105724'), -1), ceil(toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639930'), -1), roundBankers(toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639934'), -1);
SELECT ceil(toUInt128('340282366920938463463374607431768211447'), -1), round(toUInt128('340282366920938463463374607431768211453'), -1), roundBankers(toUInt128('340282366920938463463374607431768211454'), s), ceil(toUInt128('340282366920938463463374607431768211447'), s) FROM values('s Int8', -1);

SELECT x, floor(x, s), ceil(x, s), round(x, s), roundBankers(x, s) FROM values('x Int32, s Int8', (-1700000000, -9), (1700000000, -9)) ORDER BY x;
SELECT floor(materialize(toDecimal32('0.5', 9)), -32767), trunc(toDecimal128('0.5', 38), s) FROM values('s Int16', -32767);
