#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: exercises the `disk(...)` dynamic disk function, not compiled into the fast-test build.
#
# The configuration of a dynamic disk expands `${key}` property references when it is read, so a credential
# or header field with a literal-looking value such as `'${name}'` reads whatever the referenced key holds --
# with `name = 'from_env SECRET'`, a server-side value. Such a value must be treated as indirect, like
# `from_env`/`from_zk`, for every field, not only for the backend type.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"
TABLE="gcs_property_reference_credential_${DB}"

# A native GCS credential field is never accepted indirectly, even with the credential opt-in.
$CLICKHOUSE_CLIENT --use_native_gcs=1 --s3_allow_server_credentials_in_user_queries=1 -m -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DB}_secret', type = gcs,
        endpoint = 'https://storage.googleapis.com/${DB}_secret/',
        google_adc_client_id = 'id', google_adc_refresh_token = 'token', google_adc_client_secret = '\${name}'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DB}_token_uri', type = object_storage, object_storage_type = gcs,
        endpoint = 'https://storage.googleapis.com/${DB}_token_uri/',
        google_adc_client_id = 'id', google_adc_client_secret = 'secret', google_adc_refresh_token = 'token',
        google_adc_token_uri = '\${name}'); -- { serverError ACCESS_DENIED }
"

# A header with a property reference may carry server-side authentication, so it relies on server credentials.
$CLICKHOUSE_CLIENT --use_native_gcs=1 --s3_allow_server_credentials_in_user_queries=0 -m -q "
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DB}_header', type = gcs,
        endpoint = 'https://storage.googleapis.com/${DB}_header/',
        no_sign_request = 1, header = '\${name}'); -- { serverError ACCESS_DENIED }
    CREATE TABLE ${TABLE} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(name = '${DB}_s3_secret', type = s3,
        endpoint = 'http://localhost:11111/test/${DB}_s3_secret/',
        access_key_id = 'clickhouse', secret_access_key = '\${name}'); -- { serverError ACCESS_DENIED }
"

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '${TABLE}'"
