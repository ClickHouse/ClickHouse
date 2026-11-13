-- Tags: no-fasttest
-- Each key of a disk definition can be specified only once.

DROP TABLE IF EXISTS t_dynamic_disk_repeated_key;

CREATE TABLE t_dynamic_disk_repeated_key (x UInt8) ENGINE = MergeTree ORDER BY tuple()
SETTINGS disk = disk(name = 'd_dynamic_disk_repeated_key', type = s3, type = local, endpoint = 'http://127.0.0.1:1/b/',
    no_sign_request = 1); -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_dynamic_disk_repeated_key (x UInt8) ENGINE = MergeTree ORDER BY tuple()
SETTINGS disk = disk(name = 'd_dynamic_disk_repeated_key', type = s3, endpoint = 'http://127.0.0.1:1/b/',
    endpoint = 'http://127.0.0.1:2/b/', no_sign_request = 1); -- { serverError BAD_ARGUMENTS }

-- A repeated key in a nested disk.
CREATE TABLE t_dynamic_disk_repeated_key (x UInt8) ENGINE = MergeTree ORDER BY tuple()
SETTINGS disk = disk(name = 'd_dynamic_disk_repeated_key_encrypted', type = encrypted, key = '1234567812345678',
    disk = disk(name = 'd_dynamic_disk_repeated_key', type = s3, endpoint = 'http://127.0.0.1:1/b/',
        no_sign_request = 1, no_sign_request = 0)); -- { serverError BAD_ARGUMENTS }
