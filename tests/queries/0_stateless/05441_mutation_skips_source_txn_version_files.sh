#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree
# no-parallel, no-fasttest: the test arms server-wide failpoints.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CURDIR"/transactions.lib
# shellcheck source=./mergetree_mutations.lib
. "$CURDIR"/mergetree_mutations.lib

# A Wide part in full storage: updating one of its two columns hardlinks the rest of the source part's files.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (k Int32, v Int32) ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0"

trap '
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT version_metadata_store_pause_before_rename" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_before_rename_part" 2>/dev/null || true
' EXIT

tx 1 "BEGIN TRANSACTION"
tx 1 "INSERT INTO t SETTINGS async_insert = 0 VALUES (1, 1), (2, 2), (3, 3)"

# COMMIT stamps the part's creation CSN and stops with txn_version.txt.tmp written but not renamed yet.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT version_metadata_store_pause_before_rename"
tx_async 1 "COMMIT"
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT version_metadata_store_pause_before_rename PAUSE"

# A non-transactional mutation copies the part's files while that file exists and commits the copy.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_mutate_task_pause_before_rename_part"
$CLICKHOUSE_CLIENT --mutations_sync 0 -q "ALTER TABLE t UPDATE v = v + 10 WHERE 1"
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_mutate_task_pause_before_rename_part PAUSE"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT version_metadata_store_pause_before_rename"
tx_wait 1
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_before_rename_part"
wait_for_all_mutations "t"

# The mutated part has to load as committed data.
$CLICKHOUSE_CLIENT -q "DETACH TABLE t"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(v) FROM t"
$CLICKHOUSE_CLIENT -q "SELECT part_type, part_storage_type FROM system.parts WHERE database = currentDatabase() AND table = 't' AND active"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"
