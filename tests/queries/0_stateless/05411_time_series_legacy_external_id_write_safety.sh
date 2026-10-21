#!/usr/bin/env bash
# Tags: no-replicated-database
# Plain `DETACH TABLE` exercises replay of metadata without a declared ID.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

prefix="${CLICKHOUSE_TEST_UNIQUE_NAME//[^a-zA-Z0-9_]/_}"
tables=()
detached_tables=()
query()
{
    ${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table=1 --max_parallel_replicas=1 -q "$1" "${@:2}"
}
cleanup()
{
    for table in ${detached_tables[@]+"${detached_tables[@]}"}; do
        query "ATTACH TABLE ${table}" >/dev/null 2>&1 || true
    done
    for table in ${tables[@]+"${tables[@]}"}; do
        query "DROP TABLE IF EXISTS ${table}" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT
assert_one()
{
    local result
    result=$(query "$1")
    [[ "$result" == 1 ]] || { echo "Unexpected result: ${result}; query: $1" >&2; return 1; }
}
assert_no_id_setting()
{
    assert_one "SELECT count() = 1 AND max(position(create_table_query, 'id_type = ')) = 0
        FROM system.tables WHERE database = currentDatabase() AND name = '$1'"
}
create_samples()
{
    query "CREATE TABLE $1
        (id $2, timestamp DateTime64(3), value Float64)
        ENGINE = MergeTree ORDER BY (id, timestamp)"
}
create_tags()
{
    query "CREATE TABLE $1
        (id $2 DEFAULT to$2($3), metric_name LowCardinality(String), tags Map(LowCardinality(String), String))
        ENGINE = ReplacingMergeTree ORDER BY (metric_name, id)"
}
create_time_ranges()
{
    query "CREATE TABLE $1
        (id $2, min_time SimpleAggregateFunction(min, DateTime64(3)),
         max_time SimpleAggregateFunction(max, DateTime64(3)))
        ENGINE = AggregatingMergeTree ORDER BY id"
}
expect_bad_type()
{
    local label="$1" statement="$2" output
    if output=$(query "$statement" 2>&1); then
        echo "${label}: unexpectedly succeeded" >&2
        return 1
    fi
    if [[ "$output" != *BAD_TYPE_OF_FIELD* || "$output" != *UInt32* || "$output" != *UInt64* ]]; then
        echo "${label}: unexpected exception: ${output}" >&2
        return 1
    fi
    echo "${label}: rejected"
}

for kind in samples recent time_ranges tags; do
    owner="${prefix}_${kind}"
    samples="${owner}_samples"
    recent="${owner}_recent"
    tags="${owner}_tags"
    time_ranges="${owner}_time_ranges"
    metrics="${owner}_metrics"
    target="$samples"
    [[ "$kind" != recent ]] || target="$recent"
    [[ "$kind" != tags ]] || target="$tags"
    [[ "$kind" != time_ranges ]] || target="$time_ranges"
    original="${target}_original"
    bad="${target}_bad"
    tables+=("$owner" "$samples" "$recent" "$tags" "$time_ranges" "$metrics" "$original" "$bad")
    create_samples "$samples" UInt64
    create_samples "$recent" UInt64
    create_tags "$tags" UInt64 4294967297
    create_time_ranges "$time_ranges" UInt64
    query "CREATE TABLE ${metrics}
        (metric_family String, type LowCardinality(String), unit LowCardinality(String), help String) ENGINE = Memory"
    attach_uuid=$(query 'SELECT generateUUIDv4()')
    query "ATTACH TABLE ${owner} UUID '${attach_uuid}' ENGINE = TimeSeries
        SETTINGS version = 9, recent_samples_ttl_seconds = 864000, store_time_ranges = 1
        SAMPLES ${samples} RECENT SAMPLES ${recent} TAGS ${tags} TIME RANGES ${time_ranges}
        METRIC FAMILIES ${metrics}" --send_logs_level=error
    assert_no_id_setting "$owner"
    query "RENAME TABLE ${target} TO ${original}"
    if [[ "$kind" == tags ]]; then
        create_tags "$target" UInt32 1
    elif [[ "$kind" == time_ranges ]]; then
        create_time_ranges "$target" UInt32
    else
        create_samples "$target" UInt32
    fi
    insert_query="INSERT INTO ${owner} (metric_name, tags, samples)
        VALUES ('m', map(), [(now64(3), 1.)])"
    expect_bad_type "legacy ${kind} insert" "$insert_query"
    detached_tables+=("$owner")
    query "DETACH TABLE ${owner}"
    query "ATTACH TABLE ${owner}"
    assert_no_id_setting "$owner"
    expect_bad_type "legacy ${kind} replay insert" "$insert_query"
    assert_one "SELECT (SELECT count() FROM ${tags}) = 0
        AND (SELECT count() FROM ${samples}) = 0 AND (SELECT count() FROM ${recent}) = 0
        AND (SELECT count() FROM ${time_ranges}) = 0
        AND (SELECT count() FROM ${original}) = 0"
    echo "legacy ${kind} failed inserts: no writes"

    query "RENAME TABLE ${target} TO ${bad}"
    if [[ "$kind" == tags ]]; then
        create_tags "$target" UInt64 4294967297
    elif [[ "$kind" == time_ranges ]]; then
        create_time_ranges "$target" UInt64
    else
        create_samples "$target" UInt64
    fi
    query "$insert_query"
    query "SELECT samples FROM ${owner} FORMAT Null"
    assert_one "SELECT count() = 1 AND min(id) = 4294967297 AND max(id) = 4294967297 FROM ${tags}"
    assert_one "SELECT count() = 1 AND min(id) = 4294967297 AND max(id) = 4294967297 FROM ${time_ranges}"
    for physical in "$samples" "$recent"; do
        assert_one "SELECT min(id) = 4294967297 AND max(id) = 4294967297 AND count() = 1 FROM ${physical}"
    done
    assert_no_id_setting "$owner"
    echo "legacy ${kind} same-type replacement: high ID preserved, no ID pin"
done
