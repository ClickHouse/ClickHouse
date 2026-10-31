#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-shared-merge-tree
# no-fasttest: requires S3 and Keeper.
# no-parallel: pauses server-wide outdated-part loading and shutdown failpoints.
# no-shared-merge-tree: exercises zero-copy cleanup in `ReplicatedMergeTree`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

database="${CLICKHOUSE_DATABASE}_drop_cancellation"
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_drop"
output="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.out"
error="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"
client_pid=""
failpoints=(mt_pause_before_loading_outdated_part merge_tree_outdated_parts_loading_cancelled
            merge_tree_load_outdated_parts_pause)

query()
{
    # shellcheck disable=SC2086
    timeout 120 $CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=fatal \
        --enable_filesystem_cache=0 --use_page_cache_for_object_storage=0 \
        --use_page_cache_for_disks_without_file_cache=0 --insert_keeper_fault_injection_probability=0 "$@"
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
    query --query "DROP DATABASE IF EXISTS ${database} SYNC" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

# An `Ordinary` database performs the drop on the foreground query thread.
# The `Memory` row also proves that the server was not restarted after the drop.
query --allow_deprecated_database_ordinary=1 --query "
    CREATE DATABASE ${database} ENGINE=Ordinary;
    CREATE TABLE ${database}.survival (n UInt8) ENGINE=Memory;
    INSERT INTO ${database}.survival VALUES (1);
    CREATE TABLE ${database}.replica (n UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/drop_cancellation', 'r1') ORDER BY n
    SETTINGS storage_policy='s3_no_cache', allow_remote_fs_zero_copy_replication=1,
             old_parts_lifetime=3600, min_bytes_for_wide_part=0,
             prewarm_mark_cache=0, prewarm_primary_key_cache=0;
    SYSTEM STOP CLEANUP ${database}.replica;
    INSERT INTO ${database}.replica VALUES (1);
    INSERT INTO ${database}.replica VALUES (2);
    OPTIMIZE TABLE ${database}.replica FINAL;
"
[[ $(query --query "SELECT count() >= 2 FROM system.parts WHERE database='${database}' AND table='replica' AND NOT active") == 1 ]]

query --query "
    DETACH TABLE ${database}.replica;
    SYSTEM ENABLE FAILPOINT mt_pause_before_loading_outdated_part;
    ATTACH TABLE ${database}.replica;
    SYSTEM WAIT FAILPOINT mt_pause_before_loading_outdated_part PAUSE;
    SYSTEM ENABLE FAILPOINT merge_tree_outdated_parts_loading_cancelled;
    SYSTEM ENABLE FAILPOINT merge_tree_load_outdated_parts_pause;
"

query --query_id "$query_id" --query "DROP TABLE ${database}.replica" >"$output" 2>"$error" &
client_pid=$!

# Keep the outdated parts unloaded until shutdown has cancelled their background task.
# The foreground drop must then load them synchronously before removing their zero-copy locks.
query --query "
    SYSTEM WAIT FAILPOINT merge_tree_outdated_parts_loading_cancelled PAUSE;
    SYSTEM DISABLE FAILPOINT mt_pause_before_loading_outdated_part;
    SYSTEM DISABLE FAILPOINT merge_tree_outdated_parts_loading_cancelled;
    SYSTEM WAIT FAILPOINT merge_tree_load_outdated_parts_pause PAUSE;
    KILL QUERY WHERE query_id='${query_id}' ASYNC FORMAT Null;
"
[[ $(query --query "SELECT count() FROM system.processes WHERE query_id='${query_id}' AND is_cancelled") == 1 ]]
query --query "SYSTEM DISABLE FAILPOINT merge_tree_load_outdated_parts_pause"

result=0
wait "$client_pid" || result=$?
client_pid=""
if (( result != 0 )) && ! grep -q 'QUERY_WAS_CANCELLED' "$error"; then
    cat "$output" "$error"
    echo "Unexpected drop result: ${result}"
    exit 1
fi

query --query "
    EXISTS TABLE ${database}.replica;
    SELECT n FROM ${database}.survival;
"
