#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: exercises the `disk(...)` dynamic disk function, not compiled into the fast-test build.
#
# The configuration of a dynamic disk expands `${key}` property references when it is read, so
# `type = '${name}', name = 'gcs'` builds a native GCS disk although the literal `type` looks like some other
# backend. Such a backend type must be treated as indirect, like `from_env`/`from_zk`: without explicit
# credentials the disk would fall back to Application Default Credentials (or, for S3, to the server's
# environment credentials) and authenticate with the server's identity.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"
TABLE="gcs_property_reference_${DB}"

$CLICKHOUSE_CLIENT --use_native_gcs=1 --s3_allow_server_credentials_in_user_queries=0 -m -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(type = '\${name}', name = 'gcs',
        endpoint = 'https://storage.googleapis.com/${DB}_type/'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(type = object_storage, object_storage_type = '\${name}', name = 'gcs',
        endpoint = 'https://storage.googleapis.com/${DB}_object_storage_type/'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(type = '\${name}', name = 's3',
        endpoint = 'http://localhost:11111/test/${DB}_s3/'); -- { serverError ACCESS_DENIED }
"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '${TABLE}'"
