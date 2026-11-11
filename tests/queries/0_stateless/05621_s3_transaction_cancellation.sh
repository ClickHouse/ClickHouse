#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-shared-merge-tree
# no-fasttest: requires transactions, S3 and Keeper.
# no-parallel: pauses server-wide insert, mutation and rollback failpoints.
# no-shared-merge-tree: exercises local part rollback on S3 and encrypted S3.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

set -euo pipefail

database="${CLICKHOUSE_DATABASE}_transaction_cancellation"
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_insert"
output="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.out"
error="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"
client_pid=""
killer_pid=""
mutation_failpoint=mt_mutation_pause_after_part_rename
mutation_output="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_mutation.log"
killer_output="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_killer.log"
part_failpoint=merge_tree_sink_after_commit_part
rollback_failpoint=transaction_rollback_pause_after_mark

query()
{
    # shellcheck disable=SC2086
    timeout 120 $CLICKHOUSE_CLIENT --allow_repeated_settings --async_insert=0 \
        --implicit_transaction=0 --enable_filesystem_cache=0 --enable_filesystem_cache_on_write_operations=0 \
        --use_page_cache_for_object_storage=0 --use_page_cache_for_disks_without_file_cache=0 \
        --enable_blob_storage_log=1 --enable_blob_storage_log_for_read_operations=1 "$@"
}

cleanup()
{
    query --query "SYSTEM DISABLE FAILPOINT ${part_failpoint}; SYSTEM DISABLE FAILPOINT ${rollback_failpoint}; SYSTEM DISABLE FAILPOINT ${mutation_failpoint}" >/dev/null 2>&1 ||:
    if [[ -n "$client_pid" ]]; then
        query --query "KILL QUERY WHERE query_id='${query_id}' ASYNC FORMAT Null" >/dev/null 2>&1 ||:
        wait "$client_pid" ||:
    fi
    if [[ -n "$killer_pid" ]]; then
        wait "$killer_pid" ||:
    fi
    query --query "DROP DATABASE IF EXISTS ${database} SYNC" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

query --query "
    CREATE DATABASE ${database} ENGINE=Atomic;
    CREATE TABLE ${database}.survival (n UInt8) ENGINE=Memory;
    INSERT INTO ${database}.survival VALUES (1);
"
for method in kill timeout; do
    query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${method}"
    expected_error=QUERY_WAS_CANCELLED
    timeout_seconds=0
    if [[ "$method" == timeout ]]; then
        expected_error=TIMEOUT_EXCEEDED
        timeout_seconds=5
    fi
    query --query "
        CREATE TABLE ${database}.txn (n UInt64) ENGINE=MergeTree ORDER BY n
        SETTINGS storage_policy='s3_no_cache', prewarm_mark_cache=0, prewarm_primary_key_cache=0;
        SYSTEM STOP MERGES ${database}.txn;
        INSERT INTO ${database}.txn VALUES (1);
        SYSTEM ENABLE FAILPOINT ${part_failpoint};
    "
    query --query_id "$query_id" --max_execution_time="$timeout_seconds" --timeout_overflow_mode=throw \
        --query "INSERT INTO ${database}.txn SELECT 42 SETTINGS implicit_transaction=1,
        max_threads=1, max_insert_threads=1, apply_mutations_on_fly=0" >"$output" 2>"$error" &
    client_pid=$!
    query --query "SYSTEM WAIT FAILPOINT ${part_failpoint} PAUSE"
    part_and_tid=$(query --query "SELECT name, creation_tid FROM system.parts
        WHERE database='${database}' AND table='txn' AND active AND creation_csn=0")
    IFS=$'\t' read -r part tid <<< "$part_and_tid"
    [[ -n "$part" && -n "$tid" ]]
    [[ $(query --query "SELECT n FROM ${database}.txn SETTINGS implicit_transaction=1") == 1 ]]
    if [[ "$method" == kill ]]; then
        query --query "KILL QUERY WHERE query_id='${query_id}' ASYNC FORMAT Null"
    fi
    deadline=$((SECONDS + 60))
    while [[ $(query --query "SELECT count() FROM system.processes WHERE query_id='${query_id}' AND is_cancelled") != 1 ]]; do
        (( SECONDS < deadline )) || exit 1
        sleep 0.1
    done

    # Observe this rollback directly; an unrelated S3 read must not consume the pause.
    query --query "
        SYSTEM ENABLE FAILPOINT ${rollback_failpoint};
        SYSTEM DISABLE FAILPOINT ${part_failpoint};
        SYSTEM WAIT FAILPOINT ${rollback_failpoint} PAUSE;
    "
    deadline=$((SECONDS + 60))
    while [[ $(query --query "SELECT count() FROM system.transactions WHERE tid=${tid} AND state='ROLLED_BACK'") != 1 ]]; do
        (( SECONDS < deadline )) || exit 1
        sleep 0.1
    done
    query --query "SYSTEM DISABLE FAILPOINT ${rollback_failpoint}"
    result=0
    wait "$client_pid" || result=$?
    client_pid=""
    if (( result == 0 )) || ! grep -q "$expected_error" "$error" || [[ -s "$output" ]]; then
        cat "$output" "$error"
        echo "Expected ${expected_error}, client exit status: ${result}"
        exit 1
    fi
    deadline=$((SECONDS + 60))
    while [[ $(query --query "SELECT count() FROM system.processes WHERE query_id='${query_id}'") != 0 ]]; do
        (( SECONDS < deadline )) || exit 1
        sleep 0.1
    done
    [[ $(query --query "SELECT n FROM ${database}.survival") == 1 ]]
    [[ $(query --query "SELECT count() FROM system.transactions WHERE tid=${tid}") == 0 ]]
    [[ $(query --query "SELECT n FROM ${database}.txn SETTINGS implicit_transaction=1") == 1 ]]
    [[ $(query --query "SELECT count() FROM system.parts
        WHERE database='${database}' AND table='txn' AND name='${part}' AND active") == 0 ]]
    query --query "SYSTEM FLUSH LOGS transactions_info_log, blob_storage_log"
    [[ $(query --query "SELECT count() FROM system.transactions_info_log WHERE tid=${tid} AND type='Rollback'") == 1 ]]
    # The cancelled query still read its version metadata while unwinding under the blocker.
    [[ $(query --query "SELECT count() > 0 FROM system.blob_storage_log
        WHERE query_id='${query_id}' AND event_type='Read' AND error_code=0
        AND endsWith(local_path, '/${part}/txn_version.txt')") == 1 ]]
    query --query "INSERT INTO ${database}.txn VALUES (7)"
    [[ $(query --query "SELECT arraySort(groupArray(n)) FROM ${database}.txn SETTINGS implicit_transaction=1") == '[1,7]' ]]
    query --query "DROP TABLE ${database}.txn SYNC"
    echo "${method}: rollback completed, data preserved, server survived"
done

# Cancellation of a background mutation must also finish mandatory metadata rollback.
query --query "
    CREATE TABLE ${database}.local_rollback (n UInt64) ENGINE=MergeTree ORDER BY tuple()
    SETTINGS storage_policy='s3_no_cache_encrypted',
             max_bytes_to_merge_at_max_space_in_pool=0,
             old_parts_lifetime=3600, remove_rolled_back_parts_immediately=0,
             cleanup_delay_period=100000, max_cleanup_delay_period=100000,
             cleanup_delay_period_random_add=0, merge_tree_clear_old_parts_interval_seconds=100000;
    INSERT INTO ${database}.local_rollback VALUES (1), (2);
"
tx 1 "BEGIN TRANSACTION"
transaction_id=$(tx 1 "SELECT transactionID()" | cut -f2-)
query --query "SYSTEM ENABLE FAILPOINT ${mutation_failpoint}"
tx_async 1 "ALTER TABLE ${database}.local_rollback UPDATE n=n+10 WHERE 1 SETTINGS mutations_sync=1" > "$mutation_output"
query --query "SYSTEM WAIT FAILPOINT ${mutation_failpoint} PAUSE"
part=$(query --query "SELECT result_part_name FROM system.merges WHERE database='${database}' AND table='local_rollback' AND is_mutation")
[[ -n "$part" ]]

# The local transaction owns the renamed part but has not registered it with
# `MergeTreeTransaction`. Cancellation must not interrupt its metadata rollback.
# `SYSTEM STOP MERGES` synchronously sets the task's cancellation predicate without
# acquiring the parts lock or the background processing mutex.
query --query "SYSTEM STOP MERGES ${database}.local_rollback"
query --query "KILL TRANSACTION WHERE tid=${transaction_id} FORMAT Null" > "$killer_output" 2>&1 &
killer_pid=$!

# Rollback marks the transaction cancelled before trying to kill its mutations.
# Wait for that state change, not for `killMutation` to finish: a background selector
# can hold its mutex while waiting for the parts lock held by the paused task.
# A transaction already removed from `system.transactions` has finished rollback.
deadline=$((SECONDS + 60))
running=1
while (( SECONDS < deadline )); do
    running=$(query --query "
        SELECT count() FROM system.transactions
        WHERE tid=${transaction_id} AND state != 'ROLLED_BACK';
    ")
    if [[ "$running" -eq 0 ]]; then break; fi
    sleep 0.1
done
[[ "$running" -eq 0 ]]

query --query "SYSTEM DISABLE FAILPOINT ${mutation_failpoint}"
wait "$killer_pid"
killer_pid=""
tx_wait 1
grep -q UNFINISHED "$mutation_output"
tx 1 "ROLLBACK"

deadline=$((SECONDS + 60))
while [[ $(query --query "SELECT count() FROM system.merges WHERE database='${database}' AND table='local_rollback'") -gt 0 ]]; do
    (( SECONDS < deadline ))
    sleep 0.1
done

query --query "
    SELECT 'rolled back part', countIf(creation_csn=18446744073709551615), countIf(active)
    FROM system.parts WHERE database='${database}' AND table='local_rollback' AND name='${part}';
    SELECT 'rows', arraySort(groupArray(n)) FROM ${database}.local_rollback SETTINGS implicit_transaction=1;
    INSERT INTO ${database}.local_rollback VALUES (3);
    SELECT 'after insert', arraySort(groupArray(n)) FROM ${database}.local_rollback SETTINGS implicit_transaction=1;
    DROP TABLE ${database}.local_rollback SYNC;
"
