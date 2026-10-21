#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# The recent-samples selector needs ANTLR4, which is disabled in the fast-test build.
# Plain `DETACH TABLE` is required to test deferred metadata replay.

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

create_samples_target()
{
    query "CREATE TABLE $1
        (
            id $2,
            timestamp DateTime64(3),
            value Float64
        ) ENGINE = MergeTree ORDER BY (id, timestamp)"
}

for kind in samples recent; do
    owner="${prefix}_${kind}"
    samples="${owner}_samples"
    recent="${owner}_recent"
    target="$samples"
    [[ "$kind" != recent ]] || target="$recent"
    original="${target}_original"
    bad="${target}_bad"
    # Owners must be dropped before their external targets.
    tables+=("$owner" "$samples" "$recent" "$original" "$bad")

    create_samples_target "$samples" UInt64
    create_samples_target "$recent" UInt64
    query "CREATE TABLE ${owner} ENGINE = TimeSeries
        SETTINGS version = 9, recent_samples_ttl_seconds = 864000, store_time_ranges = 0
        SAMPLES ${samples} RECENT SAMPLES ${recent}
        TAGS INNER COLUMNS (id UInt64 DEFAULT toUInt64(4294967297))"
    assert_no_id_setting "$owner"

    query "RENAME TABLE ${target} TO ${original}"
    create_samples_target "$target" UInt32
    read_query="SELECT samples FROM ${owner} FORMAT Null"
    if [[ "$kind" == recent ]]; then
        read_query="SELECT value FROM timeSeriesSelector(${owner}, 'm',
            now64(3) - INTERVAL 1 HOUR, now64(3) + INTERVAL 1 MINUTE)
            SETTINGS time_series_prefer_recent_samples_table = 1 FORMAT Null"
        query "SELECT samples FROM ${owner} FORMAT Null"
        echo 'recent main samples: available'
    fi
    insert_query="INSERT INTO ${owner} (metric_name, tags, samples)
        VALUES ('m', map(), [(now64(3), 1.)])"
    expect_bad_type "${kind} read" "$read_query"
    expect_bad_type "${kind} insert" "$insert_query"

    # Replay admits the metadata, but the declared ID still guards physical access.
    detached_tables+=("$owner")
    query "DETACH TABLE ${owner}"
    query "ATTACH TABLE ${owner}"
    assert_no_id_setting "$owner"
    expect_bad_type "${kind} replay read" "$read_query"
    expect_bad_type "${kind} replay insert" "$insert_query"
    assert_one "SELECT (SELECT count() FROM timeSeriesTags(${owner})) = 0
        AND (SELECT count() FROM ${samples}) = 0 AND (SELECT count() FROM ${recent}) = 0
        AND (SELECT count() FROM ${original}) = 0"
    echo "${kind} failed inserts: no writes"

    query "RENAME TABLE ${target} TO ${bad}"
    create_samples_target "$target" UInt64
    query "$insert_query"
    query "$read_query"
    assert_one "SELECT count() = 1 AND min(id) = 4294967297 AND max(id) = 4294967297
        FROM timeSeriesTags(${owner})"
    for physical in "$samples" "$recent"; do
        assert_one "SELECT min(id) = 4294967297 AND max(id) = 4294967297 AND count() = 1
            FROM ${physical}"
    done
    echo "${kind} same-type replacement: high ID preserved"
done

# A legacy all-external full ATTACH has no durable ID. Keep that metadata branch
# available, without inferring an ID from a live target or adding a setting.
owner="${prefix}_legacy"
samples="${owner}_samples"
tags="${owner}_tags"
metrics="${owner}_metrics"
tables+=("$owner" "$samples" "$tags" "$metrics")
create_samples_target "$samples" UInt64
query "CREATE TABLE ${tags}
    (id UInt64 DEFAULT toUInt64(4294967297), metric_name LowCardinality(String), tags Map(LowCardinality(String), String))
    ENGINE = ReplacingMergeTree ORDER BY (metric_name, id)"
query "CREATE TABLE ${metrics}
    (metric_family String, type LowCardinality(String), unit LowCardinality(String), help String) ENGINE = Memory"
attach_uuid=$(query 'SELECT generateUUIDv4()')
query "ATTACH TABLE ${owner} UUID '${attach_uuid}' ENGINE = TimeSeries
    SETTINGS version = 9, recent_samples_ttl_seconds = 0, store_time_ranges = 0
    SAMPLES ${samples} TAGS ${tags} METRIC FAMILIES ${metrics}" --send_logs_level=error
assert_no_id_setting "$owner"
echo 'legacy full ATTACH: no ID pin'

detached_tables+=("$samples" "$tags" "$metrics" "$owner")
query "DETACH TABLE ${owner}"
for table in "$samples" "$tags" "$metrics"; do
    query "DETACH TABLE ${table}"
done
query "ATTACH TABLE ${owner}"
assert_no_id_setting "$owner"
echo 'legacy replay without targets: admitted, no ID pin'

for table in "$samples" "$tags" "$metrics"; do
    query "ATTACH TABLE ${table}"
done
query "INSERT INTO ${owner} (metric_name, tags, samples)
    VALUES ('m', map(), [(now64(3), 1.)])"
query "SELECT samples FROM ${owner} FORMAT Null"
assert_one "SELECT min(id) = 4294967297 AND max(id) = 4294967297 AND count() = 1 FROM ${samples}"
assert_no_id_setting "$owner"
echo 'legacy restored targets: high ID preserved'
