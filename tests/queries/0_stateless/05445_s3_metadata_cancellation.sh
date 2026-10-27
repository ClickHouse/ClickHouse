#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-shared-merge-tree
# no-fasttest: requires S3 and Keeper.
# no-parallel: pauses server-wide part metadata failpoints.
# no-shared-merge-tree: exercises local part loading in `MergeTree`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

database="${CLICKHOUSE_DATABASE}_metadata_cancellation"
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_cancel"
output="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.out"
error="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"
client_pid=""
failpoints=(check_data_part_before_projection_read check_data_part_before_serialization_read
            load_data_part_before_metadata_read load_projection_before_metadata_read)

query()
{
    # shellcheck disable=SC2086
    timeout 120 $CLICKHOUSE_CLIENT --enable_filesystem_cache=0 \
        --use_page_cache_for_object_storage=0 --use_page_cache_for_disks_without_file_cache=0 "$@"
}

cleanup()
{
    for failpoint in "${failpoints[@]}"; do
        query --query "SYSTEM DISABLE FAILPOINT ${failpoint}" >/dev/null 2>&1 ||:
    done
    if [[ -n "$client_pid" ]]; then
        query --query "KILL QUERY WHERE query_id='${query_id}' ASYNC FORMAT Null" >/dev/null 2>&1 ||:
        wait "$client_pid" ||:
    fi
    query --query "ATTACH TABLE IF NOT EXISTS ${database}.attaches" >/dev/null 2>&1 ||:
    query --query "DROP DATABASE IF EXISTS ${database} SYNC" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

query --query "CREATE DATABASE ${database} ENGINE=Atomic"
for table in checks attaches; do
    engine=MergeTree
    if [[ "$table" == checks ]]; then
        engine="ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/checks', 'r1')"
    fi
    query --query "
        CREATE TABLE ${database}.${table} (n UInt64, PROJECTION p (SELECT n ORDER BY n))
        ENGINE=${engine} ORDER BY n
        SETTINGS storage_policy='s3_no_cache', min_bytes_for_wide_part=0,
                 serialization_info_version='with_types', prewarm_mark_cache=0, prewarm_primary_key_cache=0;
        SYSTEM STOP MERGES ${database}.${table};
        INSERT INTO ${database}.${table} SELECT number FROM numbers(4096);
    "
done
query --query "SYSTEM STOP FETCHES ${database}.checks"

state()
{
    query --query "
        SELECT count(), sum(n) FROM ${database}.$1;
        SELECT arraySort(groupArray(name)) FROM system.parts
        WHERE database='${database}' AND table='$1' AND active;
        SELECT arraySort(groupArray((name, is_broken, exception_code))) FROM system.projection_parts
        WHERE database='${database}' AND table='$1' AND active;
        SELECT count() FROM system.detached_parts WHERE database='${database}' AND table='$1';
        SELECT count() FROM system.replication_queue
        WHERE database='${database}' AND table='$1' AND type='GET_PART';
    "
}

checks_before=$(state checks)
attaches_before=$(state attaches)

cancel_at()
{
    local statement=$1 failpoint=$2 method=$3 gate=${4:-}
    local expected_error=QUERY_WAS_CANCELLED
    local timeout_seconds=0
    if [[ "$method" == timeout ]]; then
        expected_error=TIMEOUT_EXCEEDED
        timeout_seconds=5
    fi

    query --query "SYSTEM ENABLE FAILPOINT ${gate:-$failpoint}"
    query --query_id "$query_id" --max_threads=1 --max_execution_time="$timeout_seconds" \
        --timeout_overflow_mode=throw --query "$statement" >"$output" 2>"$error" &
    client_pid=$!

    if [[ -n "$gate" ]]; then
        query --query "SYSTEM WAIT FAILPOINT ${gate} PAUSE"
        # Arm the serialization pause only after the parent part was checked.
        query --query "SYSTEM ENABLE FAILPOINT ${failpoint}"
        query --query "SYSTEM DISABLE FAILPOINT ${gate}"
    fi
    query --query "SYSTEM WAIT FAILPOINT ${failpoint} PAUSE"

    if [[ "$method" == kill ]]; then
        query --query "KILL QUERY WHERE query_id='${query_id}' ASYNC FORMAT Null"
    fi
    local deadline=$((SECONDS + 60))
    while [[ $(query --query "SELECT count() FROM system.processes WHERE query_id='${query_id}' AND is_cancelled") != 1 ]]; do
        if (( SECONDS >= deadline )); then
            echo "Query was not cancelled at ${failpoint}"
            return 1
        fi
        sleep 0.1
    done

    query --query "SYSTEM DISABLE FAILPOINT ${failpoint}"
    local result=0
    wait "$client_pid" || result=$?
    client_pid=""
    if (( result == 0 )) || ! grep -q "$expected_error" "$error" || [[ -s "$output" ]]; then
        cat "$output" "$error"
        echo "Expected ${expected_error} at ${failpoint}, client exit status: ${result}"
        return 1
    fi
}

assert_preserved()
{
    local table=$1 before=$2 label=$3
    local after
    after=$(state "$table")
    if [[ "$after" != "$before" ]]; then
        diff -u <(printf '%s\n' "$before") <(printf '%s\n' "$after")
        return 1
    fi
    [[ $(query --query "CHECK TABLE ${database}.${table} SETTINGS check_query_single_value_result=1") == 1 ]]
    echo "${label}: data preserved"
}

cancel_at "CHECK TABLE ${database}.checks" check_data_part_before_serialization_read kill
assert_preserved checks "$checks_before" 'check part serialization'

cancel_at "CHECK TABLE ${database}.checks" check_data_part_before_serialization_read timeout \
    check_data_part_before_projection_read
assert_preserved checks "$checks_before" 'check projection serialization'

for failpoint in load_data_part_before_metadata_read load_projection_before_metadata_read; do
    query --query "DETACH TABLE ${database}.attaches SYNC"
    query --query "SYSTEM DROP PAGE CACHE"
    cancel_at "ATTACH TABLE ${database}.attaches" "$failpoint" kill
    query --query "ATTACH TABLE ${database}.attaches"
    assert_preserved attaches "$attaches_before" "$failpoint"
done
