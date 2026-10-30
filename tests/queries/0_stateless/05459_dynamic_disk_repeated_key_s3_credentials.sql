-- A key repeated in a dynamic disk description is resolved to its first occurrence by the disk,
-- so a later occurrence must not hide an S3 backend that relies on the server's own credentials.

SET s3_allow_server_credentials_in_user_queries = 0;

DROP TABLE IF EXISTS t_repeated_key;

-- The backend is S3 without credentials: rejected.
CREATE TABLE t_repeated_key (a Int32) ENGINE = MergeTree ORDER BY tuple()
SETTINGS disk = disk(type = object_storage, object_storage_type = s3, metadata_type = plain,
    endpoint = 'http://localhost:11111/test/05459/'); -- { serverError ACCESS_DENIED }

-- A repeated `type = local` is ignored by the disk, so the backend is still S3.
CREATE TABLE t_repeated_key (a Int32) ENGINE = MergeTree ORDER BY tuple()
SETTINGS disk = disk(type = object_storage, type = local, object_storage_type = s3, metadata_type = plain,
    endpoint = 'http://localhost:11111/test/05459/'); -- { serverError ACCESS_DENIED }

-- The same for a repeated `object_storage_type`.
CREATE TABLE t_repeated_key (a Int32) ENGINE = MergeTree ORDER BY tuple()
SETTINGS disk = disk(type = object_storage, object_storage_type = s3, object_storage_type = local, metadata_type = plain,
    endpoint = 'http://localhost:11111/test/05459/'); -- { serverError ACCESS_DENIED }

-- The disk uses the first, empty, `access_key_id`, so the credentials are not explicit.
CREATE TABLE t_repeated_key (a Int32) ENGINE = MergeTree ORDER BY tuple()
SETTINGS disk = disk(type = s3, endpoint = 'http://localhost:11111/test/05459/', metadata_type = plain,
    access_key_id = '', access_key_id = 'clickhouse', secret_access_key = 'clickhouse'); -- { serverError ACCESS_DENIED }

DROP TABLE IF EXISTS t_repeated_key;
