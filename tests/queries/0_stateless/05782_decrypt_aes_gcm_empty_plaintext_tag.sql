-- Tags: no-fasttest
-- Tag no-fasttest: Depends on OpenSSL

-- The ciphertext of an empty plaintext in GCM mode is only the tag, it must still be verified.
WITH
    '0123456789abcdef' AS key,
    '123456789012' AS iv,
    encrypt('aes-128-gcm', '', key, iv) AS good,
    concat(char(bitXor(ascii(good), 1)), substring(good, 2)) AS bad
SELECT
    length(good),
    decrypt('aes-128-gcm', good, key, iv) = '',
    tryDecrypt('aes-128-gcm', good, key, iv) = '',
    tryDecrypt('aes-128-gcm', bad, key, iv) IS NULL,
    tryDecrypt('aes-128-gcm', good, 'fedcba9876543210', iv) IS NULL,
    tryDecrypt('aes-128-gcm', good, key, iv, 'aad') IS NULL,
    tryDecrypt('aes-128-gcm', encrypt('aes-128-gcm', '', key, iv, 'aad'), key, iv, 'aad') = '';

SELECT decrypt('aes-128-gcm', concat(char(bitXor(ascii(c), 1)), substring(c, 2)), '0123456789abcdef', '123456789012')
FROM (SELECT encrypt('aes-128-gcm', '', '0123456789abcdef', '123456789012') AS c); -- {serverError OPENSSL_ERROR}

-- An empty string has no tag, so it is not a valid GCM ciphertext.
SELECT decrypt('aes-128-gcm', '', '0123456789abcdef', '123456789012'); -- {serverError BAD_ARGUMENTS}
SELECT decrypt('aes-128-gcm', 'short', '0123456789abcdef', '123456789012'); -- {serverError BAD_ARGUMENTS}
SELECT tryDecrypt('aes-128-gcm', c, '0123456789abcdef', '123456789012')
FROM (SELECT arrayJoin(['', 'short', encrypt('aes-128-gcm', 'text', '0123456789abcdef', '123456789012')]) AS c);

-- Rows with a NULL argument are not decrypted.
WITH '0123456789abcdef' AS key, '123456789012' AS iv, encrypt('aes-128-gcm', '', key, iv, 'aad') AS c
SELECT decrypt('aes-128-gcm', c, key, iv, aad), tryDecrypt('aes-128-gcm', c, key, iv, aad)
FROM values('aad Nullable(String)', (NULL), ('aad'))
SETTINGS short_circuit_function_evaluation_for_nulls = 0;

WITH '0123456789abcdef' AS key, '123456789012' AS iv
SELECT decrypt('aes-128-gcm', c, key, iv), tryDecrypt('aes-128-gcm', c, key, iv)
FROM (SELECT if(number = 0, NULL, encrypt('aes-128-gcm', '', key, iv)) AS c FROM numbers(2))
SETTINGS short_circuit_function_evaluation_for_nulls = 0;

-- When the tag check fails, `tryDecrypt` returns NULL and keeps none of the unauthenticated plaintext under it,
-- also when the failing row is the last one.
WITH '0123456789abcdef' AS k, '123456789012' AS iv,
     encrypt('aes-128-gcm', 'top secret', k, iv) AS c,
     concat(substring(c, 1, length(c) - 1), char(bitXor(reinterpret(substring(c, length(c), 1), 'UInt8'), 1))) AS forged
SELECT tryDecrypt('aes-128-gcm', x, k, iv) AS r, assumeNotNull(r)
FROM (SELECT arrayJoin([encrypt('aes-128-gcm', 'first', k, iv), forged, encrypt('aes-128-gcm', 'last', k, iv)]) AS x);

WITH '0123456789abcdef' AS k, '123456789012' AS iv,
     encrypt('aes-128-gcm', 'top secret', k, iv) AS c,
     concat(substring(c, 1, length(c) - 1), char(bitXor(reinterpret(substring(c, length(c), 1), 'UInt8'), 1))) AS forged
SELECT tryDecrypt('aes-128-gcm', x, k, iv) AS r, assumeNotNull(r)
FROM (SELECT arrayJoin([encrypt('aes-128-gcm', 'first', k, iv), forged]) AS x);
