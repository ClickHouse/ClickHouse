-------------------------------------------------------------------------------
-- Validate that encrypt/decrypt (and mysql versions) work against Nullable(String).
-- null gets encrypted/decrypted as null, non-null encrypted/decrypted as usual.
-------------------------------------------------------------------------------
-- using nullIf since that is the easiest way to produce `Nullable(String)` with a `null` value

SELECT aes_encrypt_mysql('aes-256-ecb', nullIf('Hello World!', 'Hello World!'), 'test_key________________________') FORMAT Null;
SELECT aes_encrypt_mysql('aes-256-ecb', toNullable('Hello World!'), 'test_key________________________') FORMAT Null;

WITH unhex('D1B43643E1D0E9390E39BA4EAE150851') as ciphertext
SELECT aes_decrypt_mysql('aes-256-ecb', nullIf(ciphertext, ciphertext), 'test_key________________________') FORMAT Null;

WITH unhex('D1B43643E1D0E9390E39BA4EAE150851') as ciphertext
SELECT aes_encrypt_mysql('aes-256-ecb', toNullable(ciphertext), 'test_key________________________') FORMAT Null;


SELECT encrypt('aes-256-ecb', nullIf('Hello World!', 'Hello World!'), 'test_key________________________') FORMAT Null;
SELECT encrypt('aes-256-gcm', nullIf('Hello World!', 'Hello World!'), 'test_key________________________', 'test_iv_____') FORMAT Null;
SELECT encrypt('aes-256-ecb', toNullable('Hello World!'), 'test_key________________________') FORMAT Null;
SELECT encrypt('aes-256-gcm', toNullable('Hello World!'), 'test_key________________________', 'test_iv_____') FORMAT Null;


WITH unhex('D1B43643E1D0E9390E39BA4EAE150851') as ciphertext
SELECT decrypt('aes-256-ecb', nullIf(ciphertext, ciphertext), 'test_key________________________') FORMAT Null;

WITH unhex('219E6478A1A3BB5B686DA4BAD70323F192EFEDCCBBD6F49E78A7E2F6') as ciphertext
SELECT decrypt('aes-256-gcm', nullIf(ciphertext, ciphertext), 'test_key________________________', 'test_iv_____') FORMAT Null;

WITH unhex('D1B43643E1D0E9390E39BA4EAE150851') as ciphertext
SELECT decrypt('aes-256-ecb', toNullable(ciphertext), 'test_key________________________') FORMAT Null;

WITH unhex('219E6478A1A3BB5B686DA4BAD70323F192EFEDCCBBD6F49E78A7E2F6') as ciphertext
SELECT decrypt('aes-256-gcm', toNullable(ciphertext), 'test_key________________________', 'test_iv_____') FORMAT Null;
