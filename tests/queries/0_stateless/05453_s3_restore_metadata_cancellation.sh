#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: requires S3.
# no-parallel: pauses server-wide restore and S3 read failpoints.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

database="${CLICKHOUSE_DATABASE}_restore_cancellation"
backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_restore"
output="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.out"
error="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"
client_pid=""
failpoints=(restore_pause_before_data_restore_tasks s3_read_before_get_object)

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
    query --query "DROP DATABASE IF EXISTS ${database} SYNC" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

query --query "
    CREATE DATABASE ${database} ENGINE=Atomic;
    CREATE TABLE ${database}.source (n UInt64) ENGINE=MergeTree ORDER BY n
        SETTINGS storage_policy='s3_no_cache', min_bytes_for_wide_part=0,
                 prewarm_mark_cache=0, prewarm_primary_key_cache=0;
    SYSTEM STOP MERGES ${database}.source;
    INSERT INTO ${database}.source VALUES (42);
    BACKUP TABLE ${database}.source TO ${backup} FORMAT Null;
"

for detach_if_broken in 0 1; do
    restore_id="${query_id}_${detach_if_broken}"
    damage_before=$(query --query "SELECT sum(value) FROM system.errors WHERE name='BACKUP_DAMAGED'")
    query --query "SYSTEM DROP PAGE CACHE; SYSTEM ENABLE FAILPOINT restore_pause_before_data_restore_tasks"
    query --query_id "$query_id" --query "
        RESTORE TABLE ${database}.source AS ${database}.restored FROM ${backup}
        SETTINGS id='${restore_id}', restore_broken_parts_as_detached=${detach_if_broken}
        FORMAT Null
    " >"$output" 2>"$error" &
    client_pid=$!

    query --query "SYSTEM WAIT FAILPOINT restore_pause_before_data_restore_tasks PAUSE"
    # The backup is local: copying it only writes to S3. The first S3 read validates
    # the restored part, after its files have been copied to the destination.
    query --query "
        SYSTEM ENABLE FAILPOINT s3_read_before_get_object;
        SYSTEM DISABLE FAILPOINT restore_pause_before_data_restore_tasks;
        SYSTEM WAIT FAILPOINT s3_read_before_get_object PAUSE;
        KILL QUERY WHERE query_id='${query_id}' ASYNC FORMAT Null;
        SYSTEM DISABLE FAILPOINT s3_read_before_get_object;
    "

    result=0
    wait "$client_pid" || result=$?
    client_pid=""
    if (( result == 124 )); then
        cat "$output" "$error"
        echo "Restore did not finish after cancellation"
        exit 1
    fi

    echo "restore_broken_parts_as_detached=${detach_if_broken}"
    query --query "
        SELECT status, error LIKE '%QUERY_WAS_CANCELLED%', error LIKE '%BACKUP_DAMAGED%'
        FROM system.backups WHERE id='${restore_id}';
        SELECT sum(value) - ${damage_before} FROM system.errors WHERE name='BACKUP_DAMAGED';
        SELECT count() FROM system.detached_parts
        WHERE database='${database}' AND table='restored' AND reason='broken-from-backup';
        DROP TABLE ${database}.restored SYNC;
    "
done

query --query "RESTORE TABLE ${database}.source AS ${database}.restored FROM ${backup} FORMAT Null"
query --query "SELECT n FROM ${database}.restored"
