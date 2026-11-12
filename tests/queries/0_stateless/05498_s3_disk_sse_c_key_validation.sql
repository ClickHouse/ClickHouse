-- Tags: no-fasttest
-- no-fasttest: needs S3 support in the build.

-- An SSE-C key that is not the base64 of a 256-bit key is rejected when the S3 client is created, before any request.

-- Non-ASCII bytes.
CREATE TABLE t_sse_c (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(type = 's3', endpoint = 'http://localhost:11111/test/05498/', no_sign_request = 1, skip_access_check = 1,
    server_side_encryption_customer_key_base64 = 'éAAA'); -- { serverError BAD_ARGUMENTS }

-- Valid base64 of 3 bytes.
CREATE TABLE t_sse_c (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(type = 's3', endpoint = 'http://localhost:11111/test/05498/', no_sign_request = 1, skip_access_check = 1,
    server_side_encryption_customer_key_base64 = 'AAAA'); -- { serverError BAD_ARGUMENTS }

-- An invalid character in a key of the right length.
CREATE TABLE t_sse_c (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(type = 's3', endpoint = 'http://localhost:11111/test/05498/', no_sign_request = 1, skip_access_check = 1,
    server_side_encryption_customer_key_base64 = 'MDEyMzQ1Njc4OTAx!jM0NTY3ODkwMTIzNDU2Nzg5MDE='); -- { serverError BAD_ARGUMENTS }

-- A valid key followed by more characters.
CREATE TABLE t_sse_c (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(type = 's3', endpoint = 'http://localhost:11111/test/05498/', no_sign_request = 1, skip_access_check = 1,
    server_side_encryption_customer_key_base64 = 'MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=!'); -- { serverError BAD_ARGUMENTS }

-- A valid key with surrounding whitespace.
CREATE TABLE t_sse_c (x UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(type = 's3', endpoint = 'http://localhost:11111/test/05498/', no_sign_request = 1, skip_access_check = 1,
    server_side_encryption_customer_key_base64 = ' MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE= '); -- { serverError BAD_ARGUMENTS }

SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_sse_c';
