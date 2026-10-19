#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# PromQL needs ANTLR4, which is disabled in the fast-test build.
# Plain `DETACH TABLE` is required to test deferred metadata replay.

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
    local label="$1" needle="$2" query="$3" output
    if output=$(${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "$query" 2>&1); then
        echo "${label}: unexpectedly succeeded" >&2
        return 1
    fi
    if [[ "$output" != *"INVALID_SETTING_VALUE"* || "$output" != *"${needle}"* ]]; then
        echo "${label}: unexpected exception: ${output}" >&2
        return 1
    fi
    echo "${label}: rejected"
}

replay_setting()
{
    local table="$1" setting="$2" old_value="$3" new_value="$4" metadata_path default_disk_path
    metadata_path=$(${CLICKHOUSE_CLIENT} -q "
        SELECT metadata_path FROM system.tables
        WHERE database = currentDatabase() AND name = '${table}' FORMAT TabSeparatedRaw")
    [[ -n "$metadata_path" ]] || { echo 'Empty disposable metadata path' >&2; return 1; }
    if [[ "$metadata_path" != /* ]]; then
        default_disk_path=$(${CLICKHOUSE_CLIENT} -q "SELECT path FROM system.disks WHERE name = 'default' FORMAT TabSeparatedRaw")
        [[ "$default_disk_path" == /* ]] || { echo 'Cannot resolve disposable metadata path' >&2; return 1; }
        metadata_path="${default_disk_path%/}/${metadata_path}"
    fi
    [[ -f "$metadata_path" ]] || { echo 'Disposable metadata file not found' >&2; return 1; }
    ${CLICKHOUSE_CLIENT} -q "DETACH TABLE ${table}"
    if [[ $(grep -o "${setting} = ${old_value}" "$metadata_path" | wc -l | tr -d ' ') != 1 ]]; then
        echo "Expected one pinned ${setting}" >&2
        return 1
    fi
    sed -i.bak "s/${setting} = ${old_value}/${setting} = ${new_value}/" "$metadata_path"
    rm -f "${metadata_path}.bak"
    ${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "ATTACH TABLE ${table}"
    [[ $(${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = '${table}'
            AND position(create_table_query, '${setting} = ${new_value}') > 0") == 1 ]]
}

for setting in samples_bucket_step_seconds recent_samples_bucket_step_seconds; do
    for value in 0 4294967296; do
        table="${prefix}_${setting}_${value}"
        tables+=("$table")
        attach_uuid=$(${CLICKHOUSE_CLIENT} -q 'SELECT generateUUIDv4()')
        expect_rejection "full ATTACH ${setting}=${value}" 'must be in the range [1, 4294967295]' "
            ATTACH TABLE ${table} UUID '${attach_uuid}' ENGINE = TimeSeries
            SETTINGS version = 10, ${setting} = ${value}"

        ${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
            CREATE TABLE ${table} ENGINE = TimeSeries SETTINGS version = 10"
        old_value=3600
        [[ "$setting" != recent_samples_bucket_step_seconds ]] || old_value=900
        replay_setting "$table" "$setting" "$old_value" "$value"
        echo "replay ${setting}=${value}: preserved"
        [[ $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${table}") == 0 ]]
        echo "tags-only read ${setting}=${value}: available"
        expect_rejection "SELECT samples ${setting}=${value}" 'must be in the range [1, 4294967295]' "SELECT samples FROM ${table} FORMAT Null"
        expect_rejection "INSERT ${setting}=${value}" 'must be in the range [1, 4294967295]' "
            INSERT INTO ${table} (metric_name, tags, samples)
            VALUES ('m', map(), [(toDateTime64(1000, 3), 1.)])"
    done
done

table="${prefix}_step_boundary"
tables+=("$table")
${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
    CREATE TABLE ${table} ENGINE = TimeSeries SETTINGS version = 10,
        samples_bucket_step_seconds = 4294967295, recent_samples_bucket_step_seconds = 4294967295"
echo 'maximum bucket steps: accepted'

table="${prefix}_ttl_replay"
tables+=("$table")
${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 -q "
    CREATE TABLE ${table} ENGINE = TimeSeries SETTINGS version = 10"
replay_setting "$table" recent_samples_ttl_seconds 345600 9223372036854775808
echo 'TTL replay: preserved'
expect_rejection 'selector TTL guard' 'must not exceed 9223372036854775807' "
    SELECT count() FROM timeSeriesSelector(${table}, 'm', 0, 200)"
expect_rejection 'PromQL TTL guard' 'must not exceed 9223372036854775807' "
    SELECT count() FROM prometheusQuery(${table}, 'm', 200) SETTINGS enable_promql_native_plan = 0"

target="${prefix}_external_target"
owner="${prefix}_external_owner"
destination="${prefix}_as_copy"
# Owners must be removed before their external target.
tables+=("$owner" "$destination" "$target")
${CLICKHOUSE_CLIENT} --multiquery --allow_experimental_time_series_table=1 -q "
    CREATE TABLE ${target}
    (
        id UUID,
        samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(DateTime64(3), Float64))),
        bucket DateTime64(3),
        min_time SimpleAggregateFunction(min, DateTime64(3)),
        max_time SimpleAggregateFunction(max, DateTime64(3))
    ) ENGINE = AggregatingMergeTree ORDER BY (id, bucket);
    CREATE TABLE ${owner} ENGINE = TimeSeries
        SETTINGS version = 10, recent_samples_ttl_seconds = 0 SAMPLES ${target};
"
replay_setting "$owner" version 10 9
for version in 9 10 DEFAULT; do
    expect_rejection "AS legacy external source version=${version}" 'Bucket-layout settings require' "
        CREATE TABLE ${destination} AS ${owner} ENGINE = TimeSeries
        SETTINGS version = ${version}, recent_samples_ttl_seconds = 0 SAMPLES INNER ENGINE = MergeTree"
done
