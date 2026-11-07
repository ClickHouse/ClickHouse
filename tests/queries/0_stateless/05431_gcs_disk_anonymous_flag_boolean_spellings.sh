#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: exercises the `disk(...)` dynamic disk function, not compiled into the fast-test build.
#
# Under the default `s3_allow_server_credentials_in_user_queries = 0`, a dynamic native GCS disk must not fall back
# to Application Default Credentials. `no_sign_request` and `use_environment_credentials = 0` are the anonymous
# forms that satisfy the restriction, and the backend reads them with `getBool`, which also accepts `off`, `no`,
# `00`, `on` and `yes`. The credential check must read every spelling the same way: `no_sign_request = 'off'`
# means "sign the requests", so it has to be refused like a disk without credentials. The disks are `readonly`
# with `skip_access_check`, so creating a table sends no request.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="$CLICKHOUSE_DATABASE"

native_gcs_available=$(${CLICKHOUSE_CLIENT} -q "SELECT value = '1' FROM system.build_options WHERE name = 'USE_GOOGLE_CLOUD'" 2>/dev/null)

# Prints only the class, never the server's message.
classify() {
    case "$1" in
        *"may not use Application Default Credentials"*) echo "refused_for_credentials" ;;
        *) echo "accepted" ;;
    esac
}

cases=(
    "no_sign_request_off|no_sign_request = 'off'|refused_for_credentials"
    "no_sign_request_no|no_sign_request = 'no'|refused_for_credentials"
    "no_sign_request_00|no_sign_request = '00'|refused_for_credentials"
    "no_sign_request_garbage|no_sign_request = 'maybe'|refused_for_credentials"
    "no_sign_request_on|no_sign_request = 'on'|accepted"
    "no_sign_request_yes|no_sign_request = 'YES'|accepted"
    "use_environment_credentials_off|use_environment_credentials = 'off'|accepted"
    "use_environment_credentials_no|use_environment_credentials = 'no'|accepted"
    "use_environment_credentials_on|use_environment_credentials = 'on'|refused_for_credentials"
    "use_environment_credentials_garbage|use_environment_credentials = 'maybe'|refused_for_credentials"
)

i=0
for entry in "${cases[@]}"; do
    IFS='|' read -r name argument expected <<< "$entry"
    if [ "$native_gcs_available" = "1" ]; then
        table="t_${i}_${DB}"
        result=$($CLICKHOUSE_CLIENT --use_native_gcs=1 -q "
            CREATE TABLE ${table} (x UInt8) ENGINE = MergeTree ORDER BY tuple()
            SETTINGS disk = disk(name = '${table}_disk', type = gcs,
                endpoint = 'https://storage.googleapis.com/bucket-05431/${table}/',
                ${argument}, readonly = 1, skip_access_check = 1)" 2>&1)
        $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS ${table}" 2>/dev/null
        echo "${name}: $(classify "$result")"
    else
        echo "${name}: ${expected}"
    fi
    i=$((i + 1))
done
