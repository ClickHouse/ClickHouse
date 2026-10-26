#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: exercises the `disk(...)` dynamic disk function, not compiled into the fast-test build.
#
# A wrapper disk (`encrypted`, `cache`) reuses an existing disk named by its `disk` field together with its
# authenticated client. When a session with `s3_allow_server_credentials_in_user_queries = 1` has created a
# native GCS custom disk that uses Application Default Credentials, a restricted session must not be able to
# borrow the server's identity by wrapping that disk. Wrapping an anonymous native GCS disk stays allowed.
# The disks are `readonly` with `skip_access_check`, so creating a table sends no request.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"

$CLICKHOUSE_CLIENT --use_native_gcs=1 --s3_allow_server_credentials_in_user_queries=1 -m -q "
    CREATE TABLE t_adc (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DB}_adc', type = gcs,
        endpoint = 'https://storage.googleapis.com/bucket-05452/${DB}_adc/', readonly = 1, skip_access_check = 1);
    CREATE TABLE t_anonymous (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DB}_anonymous', type = gcs,
        endpoint = 'https://storage.googleapis.com/bucket-05452/${DB}_anonymous/', no_sign_request = 1,
        readonly = 1, skip_access_check = 1);
"

$CLICKHOUSE_CLIENT --s3_allow_server_credentials_in_user_queries=0 -m -q "
    CREATE TABLE t_encrypted (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(type = encrypted, disk = '${DB}_adc', path = 'encrypted/', key = '0123456789012345'); -- { serverError ACCESS_DENIED }
    CREATE TABLE t_cache (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(type = cache, disk = '${DB}_adc', path = '${DB}_cache/', max_size = '1Mi'); -- { serverError ACCESS_DENIED }
    CREATE TABLE t_encrypted (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(type = encrypted, disk = '${DB}_anonymous', path = 'encrypted/', key = '0123456789012345');
    SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name;
"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_encrypted; DROP TABLE t_anonymous; DROP TABLE t_adc"
