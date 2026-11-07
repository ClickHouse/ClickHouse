#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-object-storage, no-random-merge-tree-settings, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# no-parallel, no-fasttest: the test arms server-wide failpoints.
# no-object-storage: parts on object storage are not synced file by file.
# no-ordinary-database, no-replicated-database, no-shared-merge-tree: transactions need an Atomic database and a local MergeTree.

# With fsync_after_insert = 1 and fsync_after_insert_each_part = 0 a deduplicated INSERT syncs the part
# that already holds its block. That part can have its transaction metadata rewritten at the same time:
# `txn_version.txt.tmp` is written and then renamed to `txn_version.txt`. The sync must not try to open
# the temporary file after it was renamed away.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_fsync_txn (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, non_replicated_deduplication_window = 100,
        fsync_after_insert = 1, fsync_after_insert_each_part = 0"

trap '
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT version_metadata_store_pause_before_rename" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT part_sync_files_pause_after_collect" 2>/dev/null || true
' EXIT

INSERT_QUERY="INSERT INTO t_fsync_txn SETTINGS insert_deduplicate = 1, async_insert = 0, insert_deduplication_token = 'token' SELECT number FROM numbers(100)"

tx 1 "BEGIN TRANSACTION"
tx 1 "$INSERT_QUERY"

# COMMIT stamps the part's creation CSN and stops with txn_version.txt.tmp written but not renamed yet.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT version_metadata_store_pause_before_rename"
tx_async 1 "COMMIT"
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT version_metadata_store_pause_before_rename PAUSE"

# A retry of the same block is deduplicated against that part and collects its files to sync.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT part_sync_files_pause_after_collect"
$CLICKHOUSE_CLIENT --log_comment "fsync_txn_retry" -q "$INSERT_QUERY" &
retry_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT part_sync_files_pause_after_collect PAUSE"

# The temporary file is renamed away before the retry opens the collected files.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT version_metadata_store_pause_before_rename"
tx_wait 1
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT part_sync_files_pause_after_collect"
wait $retry_pid && echo "retry succeeded"

$CLICKHOUSE_CLIENT -q "SELECT count(), sum(k) FROM t_fsync_txn"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT ProfileEvents['DuplicatedInsertedBlocks'] > 0, ProfileEvents['FileSync'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'fsync_txn_retry'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_fsync_txn"
