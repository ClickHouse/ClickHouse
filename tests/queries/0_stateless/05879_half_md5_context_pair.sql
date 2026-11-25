SELECT hex(halfMD5('')) = 'D41D8CD98F00B204', hex(halfMD5('abc')) = '900150983CD24FB0';

SELECT countIf(halfMD5(value) != reinterpretAsUInt64(reverse(substring(MD5(value), 1, 8))))
FROM
(
    SELECT repeat('a', arrayJoin([0, 1, 3, 55, 56, 63, 64, 65, 119, 120, 127, 128, 129])) AS value
);

SELECT countIf(halfMD5(value) != reinterpretAsUInt64(reverse(substring(MD5(value), 1, 8))))
FROM
(
    SELECT concat(unhex('00FF80'), toString(number), unhex('0001')) AS value
    FROM numbers_mt(200000)
)
SETTINGS max_threads = 2, max_block_size = 257;

SELECT countIf(halfMD5(value) != reinterpretAsUInt64(reverse(substring(MD5(value), 1, 8))))
FROM
(
    SELECT toFixedString(toString(number), 16) AS value
    FROM numbers_mt(200000)
)
SETTINGS max_threads = 2, max_block_size = 257;
