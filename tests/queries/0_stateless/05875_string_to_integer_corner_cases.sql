-- String and FixedString to Int8..UInt256: which strings parse, give NULL or zero, wrap around, or throw.

SELECT 'OrNull, OrZero';
SELECT x, toInt8OrNull(x), toInt16OrNull(x), toInt32OrNull(x), toInt64OrNull(x),
    toUInt8OrNull(x), toUInt16OrNull(x), toUInt32OrNull(x), toUInt64OrNull(x),
    toInt64OrZero(x), toUInt8OrZero(x), accurateCastOrNull(x, 'Int16'), CAST(x AS Nullable(UInt32))
FROM (SELECT arrayJoin([
    '', '0', '-0', '+0', '00', '007', '+', '-', '++1', '+-1', '-+1', '--1', ' 1', '1 ', '1\0', '\t1', '1\n',
    '1a', 'a1', '1.0', '1e2', '0x1', '1_000', '１', '١',
    '127', '128', '-128', '-129', '255', '256', '-1',
    '32767', '32768', '-32768', '-32769', '65535', '65536',
    '2147483647', '2147483648', '-2147483648', '-2147483649', '4294967295', '4294967296',
    '9223372036854775807', '9223372036854775808', '-9223372036854775808', '-9223372036854775809',
    '18446744073709551615', '18446744073709551616', '+18446744073709551615',
    '000000000000000000000000000000000000000127', '-000000000000000000000000000000000000000128',
    '99999999999999999999999999999']) AS x);

SELECT 'toIntN, CAST';
SELECT x, toInt8(x), toInt16(x), toInt32(x), toInt64(x), CAST(x AS Int8), accurateCastOrNull(x, 'Int8')
FROM (SELECT arrayJoin([
    '0', '-0', '+0', '007', '127', '128', '-128', '-129', '255', '256', '-1', '32768', '-32769', '65536',
    '2147483648', '-2147483649', '4294967296', '9223372036854775808', '-9223372036854775809',
    '18446744073709551616', '99999999999999999999999999999', '-000000000000000000000000000000000000000128']) AS x);

SELECT 'toUIntN, CAST';
SELECT x, toUInt8(x), toUInt16(x), toUInt32(x), toUInt64(x), CAST(x AS UInt64), accurateCast(x, 'UInt64')
FROM (SELECT arrayJoin([
    '0', '+0', '007', '255', '256', '65535', '65536', '4294967295', '4294967296', '18446744073709551615',
    '+18446744073709551615', '000000000000000000000000000000000000000255']) AS x);

SELECT 'wide, OrNull, OrZero';
SELECT x, toInt128OrNull(x), toUInt128OrNull(x), toInt256OrNull(x), toUInt256OrNull(x),
    accurateCastOrNull(x, 'Int128'), toUInt128OrZero(x), toInt256OrZero(x), CAST(x AS Nullable(UInt256))
FROM (SELECT arrayJoin([
    '', '+', '-0', '-1', '170141183460469231731687303715884105727', '170141183460469231731687303715884105728',
    '-170141183460469231731687303715884105728', '-170141183460469231731687303715884105729',
    '340282366920938463463374607431768211455', '340282366920938463463374607431768211456',
    '57896044618658097711785492504343953926634992332820282019728792003956564819967',
    '57896044618658097711785492504343953926634992332820282019728792003956564819968',
    '-57896044618658097711785492504343953926634992332820282019728792003956564819968',
    '-57896044618658097711785492504343953926634992332820282019728792003956564819969',
    '115792089237316195423570985008687907853269984665640564039457584007913129639935',
    '115792089237316195423570985008687907853269984665640564039457584007913129639936',
    repeat('9', 45), repeat('9', 80), concat('-', repeat('0', 50), '1')]) AS x);

SELECT 'wide, toIntN, CAST';
SELECT x, toInt128(x), toInt256(x), accurateCast(x, 'Int128'), accurateCast(x, 'Int256')
FROM (SELECT arrayJoin(['0', '+7', '-1', '170141183460469231731687303715884105727', '-170141183460469231731687303715884105728']) AS x);
SELECT toInt128(repeat('9', 45)), CAST('340282366920938463463374607431768211456' AS UInt128), toUInt256(repeat('9', 80));

SELECT 'FixedString';
SELECT hex(x), toInt32OrNull(x), toUInt8OrNull(x), toInt64OrZero(x), toInt128OrNull(x), accurateCastOrNull(x, 'Int16'), toUInt8OrZero(x)
FROM (SELECT arrayJoin([toFixedString('12', 4), toFixedString('', 4), toFixedString('0', 4), toFixedString('300', 4),
    toFixedString('-1', 4), toFixedString('+7', 4), toFixedString('-', 4), toFixedString('1234', 4),
    toFixedString(unhex('31003200'), 4), toFixedString(unhex('00003132'), 4)]) AS x);
SELECT toUInt8(toFixedString('300', 4)), CAST(toFixedString('-1', 3) AS Int64), toInt32(toFixedString('', 3));

SELECT 'errors';
SELECT toUInt8(materialize('-1')); -- { serverError CANNOT_PARSE_NUMBER }
SELECT CAST(materialize('-0') AS UInt32); -- { serverError CANNOT_PARSE_NUMBER }
SELECT toInt32(materialize('++1')); -- { serverError CANNOT_PARSE_NUMBER }
SELECT toInt32(materialize('+')); -- { serverError CANNOT_PARSE_NUMBER }
SELECT toInt32(materialize('')); -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
SELECT toInt32(materialize(' 1')); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast(materialize('128'), 'Int8'); -- { serverError CANNOT_PARSE_TEXT }
SELECT toInt64(arrayJoin(['1', '2', '3x'])); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast(materialize(repeat('9', 45)), 'Int128'); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast(materialize('-1'), 'UInt256'); -- { serverError CANNOT_PARSE_TEXT }
SELECT toInt32(toFixedString('1x', 4)); -- { serverError CANNOT_PARSE_TEXT }
SELECT toUInt16(toFixedString('-', 2)); -- { serverError CANNOT_PARSE_NUMBER }
