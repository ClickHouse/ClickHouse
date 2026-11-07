#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# no-parallel -- a server-wide failpoint postpones mutation selection on every table.
# no-replicated-database -- the local timing of `UPDATE`, `ALTER` and `OPTIMIZE` is assumed.
# no-shared-merge-tree -- the merged part version under test is chosen by StorageMergeTree.

# A merge applies the patch parts up to the next pending mutation and materializes the pending
# `DROP COLUMN` mutations on its own. A patch it does not apply, because a mutation is pending below
# it or because `apply_patches_on_merge` is off, bounds the version the merged part records: a part
# at or above the patch's version would never have the patch applied, and the update would be lost.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_reservations.lib
. "$CUR_DIR"/mergetree_reservations.lib

MUTATE_FP="mt_select_parts_to_mutate_no_free_threads"
TABLE="t_merge_past_unapplied_patch"
MUTATIONS="FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE'"
PARTS="FROM system.parts WHERE database = currentDatabase() AND table = '$TABLE' AND active"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $MUTATE_FP"
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC"
}
trap cleanup EXIT

function fresh_table()
{
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS $TABLE SYNC;
        CREATE TABLE $TABLE (id UInt64, v UInt64, d1 UInt64, d2 UInt64) ENGINE = MergeTree ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, min_bytes_for_wide_part = 0, max_bytes_to_merge_at_max_space_in_pool = 1, $1;
        INSERT INTO $TABLE SELECT number, 1, 7, 7 FROM numbers(1000);
        INSERT INTO $TABLE SELECT number + 1000, 1, 7, 7 FROM numbers(1000);
    "
}

# The merged part must stay below the patch, so that the patch still applies to it.
function check_merge()
{
    $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $TABLE FINAL"
    $CLICKHOUSE_CLIENT --query "SELECT 'regular parts:', count() $PARTS AND NOT startsWith(partition_id, 'patch-')"
    $CLICKHOUSE_CLIENT --query "
        SELECT 'merged part below the patch:',
            (SELECT max(data_version) $PARTS AND NOT startsWith(partition_id, 'patch-'))
            < (SELECT min(data_version) $PARTS AND startsWith(partition_id, 'patch-'))"
    $CLICKHOUSE_CLIENT --query "SELECT 'rows by v:', v, count() FROM $TABLE GROUP BY v ORDER BY v"
}

function run_mutations()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $MUTATE_FP"
    wait_for_query_result "SELECT count() $MUTATIONS AND NOT is_done" "0"
    $CLICKHOUSE_CLIENT --query "SELECT 'rows by v after the mutations:', v, count() FROM $TABLE GROUP BY v ORDER BY v"
}

echo "--- the patch lies between two pending DROP COLUMN mutations"
fresh_table "apply_patches_on_merge = 1"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $MUTATE_FP"
$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE DROP COLUMN d1 SETTINGS alter_sync = 0"
$CLICKHOUSE_CLIENT --enable_lightweight_update 1 --query "UPDATE $TABLE SET v = 2 WHERE 1"
$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE DROP COLUMN d2 SETTINGS alter_sync = 0"
# The merge applies no patch above the first pending mutation, so it must record a version below the patch.
check_merge
run_mutations

echo "--- the merge applies no patches at all"
fresh_table "apply_patches_on_merge = 0"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $MUTATE_FP"
$CLICKHOUSE_CLIENT --enable_lightweight_update 1 --query "UPDATE $TABLE SET v = 2 WHERE 1"
$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE DROP COLUMN d1 SETTINGS alter_sync = 0"
check_merge
run_mutations

echo "--- a merge that would materialize a RENAME COLUMN registered above an unapplied patch is refused"
fresh_table "apply_patches_on_merge = 0"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $MUTATE_FP"
$CLICKHOUSE_CLIENT --enable_lightweight_update 1 --query "UPDATE $TABLE SET v = 2 WHERE 1"
$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE RENAME COLUMN d1 TO d3 SETTINGS alter_sync = 0"
# The merged part could only record a version below the patch, so the rename it wrote would run again later.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $TABLE FINAL SETTINGS optimize_throw_if_noop = 1" 2>&1 | expect_error "CANNOT_ASSIGN_OPTIMIZE" "would materialize a pending RENAME COLUMN"
run_mutations
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $TABLE FINAL SETTINGS optimize_throw_if_noop = 1"
$CLICKHOUSE_CLIENT --query "SELECT 'rows by v, d3 after the merge:', v, d3, count() FROM $TABLE GROUP BY v, d3 ORDER BY v, d3"
