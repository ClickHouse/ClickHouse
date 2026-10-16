-- `toIPv4` from `String` parses canonical addresses with the SIMD parser and falls back to the generic parser,
-- which also accepts leading zeros. Both kinds of rows are mixed in one column, so both paths are used in one block.

SELECT s, toIPv4OrNull(s), toIPv4OrZero(s)
FROM (SELECT arrayJoin(['1.2.3.4', '01.02.03.004', '256.1.1.1', '1.2.3.4 ', '127.0.0.1']) AS s);

SELECT toIPv4(arrayJoin(['1.2.3.4', '001.2.3.4']));

SELECT toIPv4(materialize('256.1.1.1')); -- { serverError CANNOT_PARSE_IPV4 }
