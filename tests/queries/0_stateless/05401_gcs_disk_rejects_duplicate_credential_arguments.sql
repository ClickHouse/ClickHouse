-- Tags: no-fasttest
-- Tag no-fasttest: exercises the `disk(...)` dynamic disk function, not compiled into the fast-test build.

-- `Poco::Util::XMLConfiguration` resolves duplicate siblings to the first one, so a repeated credential or
-- backend-type argument of a dynamic disk would let the credential checks see a different value from the one
-- the disk uses. Such repeats are rejected before the disk is created.

SET use_native_gcs = 1;

DROP TABLE IF EXISTS t_dup;

CREATE TABLE t_dup (x UInt8) ENGINE = MergeTree ORDER BY tuple()
SETTINGS disk = disk(type = gcs, endpoint = 'https://storage.googleapis.com/bucket/dup_a/',
    no_sign_request = 0, no_sign_request = 1); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_dup (x UInt8) ENGINE = MergeTree ORDER BY tuple()
SETTINGS disk = disk(type = gcs, endpoint = 'https://storage.googleapis.com/bucket/dup_b/',
    service_account_key_file = '/etc/passwd', service_account_key_file = ''); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_dup (x UInt8) ENGINE = MergeTree ORDER BY tuple()
SETTINGS disk = disk(type = object_storage, object_storage_type = gcs, object_storage_type = local,
    endpoint = 'https://storage.googleapis.com/bucket/dup_c/'); -- { serverError BAD_ARGUMENTS }

SELECT 'ok';
