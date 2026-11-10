-- CRC32, CRC32IEEE and CRC64 checksums for lengths around 64 bytes and for longer inputs with a tail.
-- Inputs contain NUL and bytes >= 0x80. Expected values come from the byte-at-a-time definitions.

SELECT 'String';
SELECT n, CRC32(s), CRC32IEEE(s), CRC64(s)
FROM
(
    SELECT n, materialize(arrayStringConcat(arrayMap(i -> char((i * 37 + 11) % 256), range(n)))) AS s
    FROM (SELECT arrayJoin([1, 7, 8, 9, 15, 16, 17, 31, 63, 64, 65, 127, 200, 4097]) AS n)
)
ORDER BY n;

SELECT 'FixedString';
WITH arrayStringConcat(arrayMap(i -> char((i * 37 + 11) % 256), range(65))) AS s
SELECT
    CRC32(materialize(toFixedString(substring(s, 1, 63), 63))), CRC32IEEE(materialize(toFixedString(substring(s, 1, 63), 63))), CRC64(materialize(toFixedString(substring(s, 1, 63), 63))),
    CRC32(materialize(toFixedString(substring(s, 1, 64), 64))), CRC32IEEE(materialize(toFixedString(substring(s, 1, 64), 64))), CRC64(materialize(toFixedString(substring(s, 1, 64), 64))),
    CRC32(materialize(toFixedString(s, 65))), CRC32IEEE(materialize(toFixedString(s, 65))), CRC64(materialize(toFixedString(s, 65)));
