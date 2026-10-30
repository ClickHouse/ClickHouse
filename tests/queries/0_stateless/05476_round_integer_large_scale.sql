-- round, roundBankers, floor, ceil and trunc on integers with |N| >= 20, where 10^|N| does not fit UInt64.

-- Constant N on an integer column.
SELECT round(number, 20), floor(number, -20), ceil(toInt32(-number), -20), trunc(toUInt8(number), -21), roundBankers(toInt64(number), -32768) FROM numbers(2);

-- 128-bit integer column, constant N.
SELECT x, floor(x, -20), ceil(x, -20), round(x, -20), roundBankers(x, -20), trunc(x, -21), round(x, 20)
FROM (SELECT arrayJoin([toInt128('100000000000000000005'), toInt128('-250000000000000000000'), toInt128('350000000000000000000')]) AS x);

-- Constant arguments.
SELECT floor(toInt128('100000000000000000005'), -20), floor(toUInt256('100000000000000000000000005'), -25), round(toInt256('-150000000000000000000'), -20);

-- Non-constant N.
SELECT s, floor(toUInt128('340000000000000000000000000000000000005'), s), round(toInt256('-150000000000000000000'), s),
    trunc(toUInt64('18446744073709551615'), s), floor(toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935'), s)
FROM values('s Int16', -20, -38, -39, -77, -78, 20);

-- The largest scales: 10^38 for Int128, 10^76 for Int256, 10^77 for UInt256.
SELECT floor(x, -38), round(y, -76), floor(z, -77), round(w, -77), floor(z, -78)
FROM (SELECT materialize(toInt128('150000000000000000000000000000000000000')) AS x, materialize(toInt256(concat('-25', repeat('0', 75)))) AS y,
    materialize(toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935')) AS z, materialize(toUInt256(concat('6', repeat('0', 76)))) AS w);
