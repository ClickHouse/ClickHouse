#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

prefix="${CLICKHOUSE_TEST_UNIQUE_NAME//[^a-zA-Z0-9_]/_}"
tables=()
cleanup()
{
    for table in "${tables[@]}"; do
        ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${table}" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT

expect_rejection()
{
    local label="$1" setting="$2" needle="$3" query="$4" output
    if output=$(${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "$query" 2>&1); then
        echo "${label}: unexpectedly succeeded" >&2
        return 1
    fi
    if [[ "$output" != *"INVALID_SETTING_VALUE"* || "$output" != *"${setting}"* || "$output" != *"${needle}"* ]]; then
        echo "${label}: unexpected exception: ${output}" >&2
        return 1
    fi
    echo "${label}: rejected"
}

for setting in samples_bucket_step_seconds recent_samples_bucket_step_seconds; do
    for value in 0 4294967296; do
        table="${prefix}_${setting}_${value}"
        tables+=("$table")
        attach_uuid=$(${CLICKHOUSE_CLIENT} -q 'SELECT generateUUIDv4()')
        expect_rejection "CREATE ${setting}=${value}" "$setting" 'must be in the range [1, 4294967295]' "
            CREATE TABLE ${table} ENGINE = TimeSeries
            SETTINGS version = 11, ${setting} = ${value}"
        expect_rejection "full ATTACH ${setting}=${value}" "$setting" 'must be in the range [1, 4294967295]' "
            ATTACH TABLE ${table} UUID '${attach_uuid}' ENGINE = TimeSeries
            SETTINGS version = 11, ${setting} = ${value}"
    done
done

table="${prefix}_step_boundary"
tables+=("$table")
${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
    CREATE TABLE ${table} ENGINE = TimeSeries SETTINGS version = 11,
        samples_bucket_step_seconds = 4294967295, recent_samples_bucket_step_seconds = 4294967295"
echo 'maximum bucket steps: accepted'

table="${prefix}_ttl_overflow"
tables+=("$table")
attach_uuid=$(${CLICKHOUSE_CLIENT} -q 'SELECT generateUUIDv4()')
expect_rejection 'CREATE TTL overflow' recent_samples_ttl_seconds 'must not exceed 9223372036854775807' "
    CREATE TABLE ${table} ENGINE = TimeSeries
    SETTINGS version = 11, recent_samples_ttl_seconds = 9223372036854775808"
expect_rejection 'full ATTACH TTL overflow' recent_samples_ttl_seconds 'must not exceed 9223372036854775807' "
    ATTACH TABLE ${table} UUID '${attach_uuid}' ENGINE = TimeSeries
    SETTINGS version = 11, recent_samples_ttl_seconds = 9223372036854775808"
