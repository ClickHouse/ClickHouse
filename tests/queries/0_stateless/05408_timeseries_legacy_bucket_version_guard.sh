#!/usr/bin/env bash
# Tags: no-replicated-database
# Plain `DETACH TABLE` is required to test deferred metadata replay.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

prefix="${CLICKHOUSE_TEST_UNIQUE_NAME//[^a-zA-Z0-9_]/_}"
scalar_v8="${prefix}_scalar_v8"
scalar_v9="${prefix}_scalar_v9"
bucket_target="${prefix}_bucket_target"
bucket_owner="${prefix}_bucket_owner"
attach_uuid=$(${CLICKHOUSE_CLIENT} -q 'SELECT generateUUIDv4()')

cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${bucket_owner}" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${prefix}_create_bad" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${prefix}_attach_bad" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${bucket_target}" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${scalar_v8}" >/dev/null 2>&1 || true
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${scalar_v9}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --multiquery --allow_experimental_time_series_table=1 -q "
    CREATE TABLE ${scalar_v8} ENGINE = TimeSeries SETTINGS version = 8;
    CREATE TABLE ${scalar_v9} ENGINE = TimeSeries SETTINGS version = 9;
    CREATE TABLE ${bucket_target}
    (
        id UUID,
        samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3), Float64))),
        bucket DateTime64(3),
        min_time SimpleAggregateFunction(min, DateTime64(3)),
        max_time SimpleAggregateFunction(max, DateTime64(3))
    ) ENGINE = AggregatingMergeTree ORDER BY (id, bucket);
"
${CLICKHOUSE_CLIENT} -q "
    SELECT extract(create_table_query, 'version = (\\d+)')
    FROM system.tables
    WHERE database = currentDatabase() AND name IN ('${scalar_v8}', '${scalar_v9}')
    ORDER BY name
"

function expect_bucket_rejection()
{
    local label="$1"
    local query="$2"
    local output
    if output=$(${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "$query" 2>&1); then
        echo "${label}: unexpectedly succeeded" >&2
        return 1
    fi
    if [[ "$output" != *"bucketed samples require version 10"* ]]; then
        echo "${label}: unexpected exception: ${output}" >&2
        return 1
    fi
    echo "${label}: rejected"
}

expect_bucket_rejection "CREATE" "
    CREATE TABLE ${prefix}_create_bad ENGINE = TimeSeries
    SETTINGS version = 9, recent_samples_ttl_seconds = 0 SAMPLES ${bucket_target}
"

expect_bucket_rejection "full ATTACH" "
    ATTACH TABLE ${prefix}_attach_bad UUID '${attach_uuid}'
    ENGINE = TimeSeries SETTINGS version = 9, recent_samples_ttl_seconds = 0
    SAMPLES ${bucket_target}
"

# Create a valid latest table, then rewrite only its disposable metadata to the legacy private version.
# Short ATTACH is metadata replay: it deliberately defers external-target validation until access.
${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
    CREATE TABLE ${bucket_owner} ENGINE = TimeSeries
    SETTINGS version = 10, recent_samples_ttl_seconds = 0 SAMPLES ${bucket_target}
"
metadata_path=$(${CLICKHOUSE_CLIENT} -q "SELECT metadata_path FROM system.tables WHERE database = currentDatabase() AND name = '${bucket_owner}' FORMAT TabSeparatedRaw")
if [[ -z "$metadata_path" ]]; then
    echo "Could not locate disposable TimeSeries metadata: empty metadata_path" >&2
    exit 1
fi
if [[ "$metadata_path" != /* ]]; then
    default_disk_path=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.disks WHERE name = 'default' FORMAT TabSeparatedRaw")
    if [[ -z "$default_disk_path" || "$default_disk_path" != /* ]]; then
        echo "Could not resolve relative TimeSeries metadata path: default disk path is '${default_disk_path}'" >&2
        exit 1
    fi
    metadata_path="${default_disk_path%/}/${metadata_path}"
fi
if [[ ! -f "$metadata_path" ]]; then
    echo "Could not locate disposable TimeSeries metadata: ${metadata_path}" >&2
    exit 1
fi
${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${bucket_owner}"
if [[ $(grep -o 'version = 10' "$metadata_path" | wc -l | tr -d ' ') != 1 ]]; then
    echo "Expected one pinned version in ${metadata_path}" >&2
    exit 1
fi
sed -i.bak 's/version = 10/version = 9/' "$metadata_path"
rm -f "${metadata_path}.bak"
${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "ATTACH TABLE ${bucket_owner}"

expect_bucket_rejection "deferred SELECT" "SELECT count() FROM ${bucket_owner}"
expect_bucket_rejection "deferred INSERT" "INSERT INTO ${bucket_owner} (metric_name, tags, samples) VALUES ('m', map(), [(toDateTime64(1000, 3), 1.)])"
