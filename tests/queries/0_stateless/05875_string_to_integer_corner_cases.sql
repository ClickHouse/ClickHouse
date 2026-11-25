-- String to Int8..UInt64: which strings parse, give NULL or zero, wrap around, or throw.

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

SELECT 'errors';
SELECT toUInt8(materialize('-1')); -- { serverError CANNOT_PARSE_NUMBER }
SELECT CAST(materialize('-0') AS UInt32); -- { serverError CANNOT_PARSE_NUMBER }
SELECT toInt32(materialize('++1')); -- { serverError CANNOT_PARSE_NUMBER }
SELECT toInt32(materialize('+')); -- { serverError CANNOT_PARSE_NUMBER }
SELECT toInt32(materialize('')); -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
SELECT toInt32(materialize(' 1')); -- { serverError CANNOT_PARSE_TEXT }
SELECT accurateCast(materialize('128'), 'Int8'); -- { serverError CANNOT_PARSE_TEXT }
SELECT toInt64(arrayJoin(['1', '2', '3x'])); -- { serverError CANNOT_PARSE_TEXT }
