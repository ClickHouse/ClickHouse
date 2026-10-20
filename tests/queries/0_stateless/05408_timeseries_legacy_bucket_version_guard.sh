#!/usr/bin/env bash

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
    if [[ "$output" != *"bucketed samples require version 11"* ]]; then
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

# Start with a valid legacy owner, then change its external target through ordinary DDL.
# Access must validate the replacement target even though the owner definition did not change.
${CLICKHOUSE_CLIENT} --multiquery -q "
    DROP TABLE ${bucket_target};
    CREATE TABLE ${bucket_target}
    (
        id UUID,
        timestamp DateTime64(3),
        value Float64
    ) ENGINE = MergeTree ORDER BY (id, timestamp);
"
${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
    CREATE TABLE ${bucket_owner} ENGINE = TimeSeries
    SETTINGS version = 9, recent_samples_ttl_seconds = 0 SAMPLES ${bucket_target}
"
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO ${bucket_owner} (metric_name, tags, samples)
    VALUES ('m', map(), [(toDateTime64(1000, 3), 1.)])
"
[[ $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${bucket_owner}") == 1 ]]
${CLICKHOUSE_CLIENT} --multiquery -q "
    DROP TABLE ${bucket_target};
    CREATE TABLE ${bucket_target}
    (
        id UUID,
        samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3), Float64))),
        bucket DateTime64(3),
        min_time SimpleAggregateFunction(min, DateTime64(3)),
        max_time SimpleAggregateFunction(max, DateTime64(3))
    ) ENGINE = AggregatingMergeTree ORDER BY (id, bucket);
"

expect_bucket_rejection "deferred SELECT" "SELECT count() FROM ${bucket_owner}"
expect_bucket_rejection "deferred INSERT" "INSERT INTO ${bucket_owner} (metric_name, tags, samples) VALUES ('m', map(), [(toDateTime64(1000, 3), 1.)])"
