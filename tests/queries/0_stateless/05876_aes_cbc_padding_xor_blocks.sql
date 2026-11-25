-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

-- Reconstruct CBC with the unchanged ECB path, including PKCS#7 padding.
WITH
    substring(repeat(unhex('00017F80FF414243'), 33), 1, n) AS plaintext,
    repeat('k', 16) AS key,
    repeat('iv', 8) AS iv,
    16 - length(plaintext) % 16 AS padding,
    concat(plaintext, repeat(char(padding), padding)) AS padded,
    arrayMap(i -> substring(padded, i * 16 + 1, 16), range(intDiv(length(padded), 16))) AS blocks,
    arrayFold(
        (acc, block) -> concat(acc, substring(encrypt('aes-128-ecb',
            arrayStringConcat(arrayMap(i -> char(bitXor(
                ascii(substring(block, i + 1, 1)),
                ascii(substring(right(acc, 16), i + 1, 1)))), range(16))), key), 1, 16)),
        blocks, iv) AS reference
SELECT countIf(encrypt('aes-128-cbc', plaintext, key, iv) != substring(reference, 17))
FROM (SELECT arrayJoin(arrayConcat(range(34), [63, 64, 65, 255, 256])) AS n);

-- Reconstruct CBC with the unchanged ECB path, including PKCS#7 padding.
WITH
    substring(repeat(unhex('00017F80FF414243'), 33), 1, n) AS plaintext,
    repeat('k', 24) AS key,
    substring(concat(toString(n), repeat('v', 16)), 1, 16) AS iv,
    16 - length(plaintext) % 16 AS padding,
    concat(plaintext, repeat(char(padding), padding)) AS padded,
    arrayMap(i -> substring(padded, i * 16 + 1, 16), range(intDiv(length(padded), 16))) AS blocks,
    arrayFold(
        (acc, block) -> concat(acc, substring(encrypt('aes-192-ecb',
            arrayStringConcat(arrayMap(i -> char(bitXor(
                ascii(substring(block, i + 1, 1)),
                ascii(substring(right(acc, 16), i + 1, 1)))), range(16))), key), 1, 16)),
        blocks, iv) AS reference
SELECT countIf(encrypt('aes-192-cbc', plaintext, key, iv) != substring(reference, 17))
FROM (SELECT arrayJoin(arrayConcat(range(34), [63, 64, 65, 255, 256])) AS n);

-- Reconstruct CBC with the unchanged ECB path, including PKCS#7 padding.
WITH
    substring(repeat(unhex('00017F80FF414243'), 33), 1, n) AS plaintext,
    repeat('k', 32) AS key,
    substring(concat(toString(n), repeat('v', 16)), 1, 16) AS iv,
    16 - length(plaintext) % 16 AS padding,
    concat(plaintext, repeat(char(padding), padding)) AS padded,
    arrayMap(i -> substring(padded, i * 16 + 1, 16), range(intDiv(length(padded), 16))) AS blocks,
    arrayFold(
        (acc, block) -> concat(acc, substring(encrypt('aes-256-ecb',
            arrayStringConcat(arrayMap(i -> char(bitXor(
                ascii(substring(block, i + 1, 1)),
                ascii(substring(right(acc, 16), i + 1, 1)))), range(16))), key), 1, 16)),
        blocks, iv) AS reference
SELECT countIf(encrypt('aes-256-cbc', plaintext, key, iv) != substring(reference, 17))
FROM (SELECT arrayJoin(arrayConcat(range(34), [63, 64, 65, 255, 256])) AS n);
