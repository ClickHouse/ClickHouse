-- cityHash64 hashes inputs of up to 16 bytes inline and longer ones out of line: the values must not change.

SELECT number AS len, offset, cityHash64(substring('The quick brown fox jumps over the lazy dog, again and again', 1 + offset, len))
FROM numbers(41) ARRAY JOIN [0, 1, 3, 7] AS offset
ORDER BY len, offset;

SELECT cityHash64(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')), cityHash64(toUUID('00000000-0000-0000-0000-000000000000'));
SELECT cityHash64(materialize(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))) FROM numbers(2);
SELECT cityHash64(toFixedString('0123456789abcdef', 16)), cityHash64(toFixedString('abc', 3));
SELECT cityHash64('a', 'bc', toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), 'a string longer than sixteen bytes');
SELECT cityHash64(toNullable('hello')), cityHash64(CAST(NULL, 'Nullable(String)'));
