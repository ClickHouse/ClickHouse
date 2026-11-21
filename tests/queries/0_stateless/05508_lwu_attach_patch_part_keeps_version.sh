#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# Tag no-parallel: server-wide failpoints keep the mutations pending.
# Tag no-fasttest: ReplicatedMergeTree needs Keeper.
# Tag no-replicated-database: extra replicas change the block numbers the scenario relies on.
# Tag no-shared-merge-tree: SharedMergeTree schedules mutations by other code.

# ATTACH of a lightweight-update patch part keeps the version of the update in the part name,
# which merges and mutations use to order the patch against the mutations of the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

trap '$CLICKHOUSE_CLIENT --query "
    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_no_free_threads;
" 2>/dev/null || true' EXIT

CLIENT="$CLICKHOUSE_CLIENT --enable_lightweight_update 1 --apply_patch_parts 1 --insert_keeper_fault_injection_probability 0"

echo "MergeTree, ATTACH PART"
# remove_unused_patch_parts = 0: the version checks read the attached patch after its update is materialized.
$CLIENT --query "
    CREATE TABLE t_mt (id UInt64, a UInt64, d UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
        max_bytes_to_merge_at_max_space_in_pool = 1, apply_patches_on_merge = 1, remove_unused_patch_parts = 0;
    INSERT INTO t_mt VALUES (1, 0, 0);
    UPDATE t_mt SET a = 5 WHERE id = 1;
"

patch=$($CLIENT --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_mt' AND active AND startsWith(name, 'patch-')")
version=$($CLIENT --query "SELECT data_version FROM system.parts WHERE database = currentDatabase() AND table = 't_mt' AND name = '$patch'")

# The merge of OPTIMIZE materializes the pending DROP COLUMN and must apply the reattached patch.
$CLIENT --query "
    ALTER TABLE t_mt DETACH PART '$patch';
    INSERT INTO t_mt VALUES (2, 0, 0);
    SYSTEM ENABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
    ALTER TABLE t_mt DROP COLUMN d SETTINGS alter_sync = 0, mutations_sync = 0;
    ALTER TABLE t_mt ATTACH PART '$patch';
    OPTIMIZE TABLE t_mt FINAL;
    SELECT id, a FROM t_mt ORDER BY id;
    SYSTEM DISABLE FAILPOINT mt_select_parts_to_mutate_no_free_threads;
"
wait_for_all_mutations t_mt
$CLIENT --query "
    SELECT id, a FROM t_mt ORDER BY id;
    SELECT 'attached patch keeps its version', data_version = $version FROM system.parts
    WHERE database = currentDatabase() AND table = 't_mt' AND active AND startsWith(name, 'patch-') AND rows > 0;
    DROP TABLE t_mt;
"

echo "ReplicatedMergeTree, ATTACH PARTITION"
$CLIENT --query "
    CREATE TABLE t_rmt (id UInt64, a UInt64, b UInt64) ENGINE = ReplicatedMergeTree('/zookeeper/{database}/t_rmt', '1') ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
        max_bytes_to_merge_at_max_space_in_pool = 1, apply_patches_on_merge = 1, remove_unused_patch_parts = 0,
        merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;
"

# Data versions come from the block numbers of partition 'all', the block number of the attached
# patch from those of its own partition: keep the latter below the versions of both mutations.
for i in 1 2 3 4 5 6; do
    $CLIENT --query "INSERT INTO t_rmt VALUES ($i, 0, 0)"
done

$CLIENT --query "
    SYSTEM ENABLE FAILPOINT rmt_merge_selecting_task_no_free_threads;
    ALTER TABLE t_rmt UPDATE a = a * 10 + 1 WHERE 1 SETTINGS alter_sync = 0, mutations_sync = 0, alter_update_mode = 'heavy';
    UPDATE t_rmt SET a = 5 WHERE id = 1;
"

patch_partition=$($CLIENT --query "SELECT any(partition_id) FROM system.parts WHERE database = currentDatabase() AND table = 't_rmt' AND active AND startsWith(partition_id, 'patch-')")
version=$($CLIENT --query "SELECT data_version FROM system.parts WHERE database = currentDatabase() AND table = 't_rmt' AND active AND partition_id = '$patch_partition'")

# The patch is between the two mutations, so they must not be executed as one.
$CLIENT --query "
    ALTER TABLE t_rmt DETACH PARTITION ID '$patch_partition';
    ALTER TABLE t_rmt UPDATE b = 1 WHERE 1 SETTINGS alter_sync = 0, mutations_sync = 0, alter_update_mode = 'heavy';
    ALTER TABLE t_rmt ATTACH PARTITION ID '$patch_partition';
    SYSTEM DISABLE FAILPOINT rmt_merge_selecting_task_no_free_threads;
"
wait_for_all_mutations t_rmt
$CLIENT --query "
    SELECT id, a, b FROM t_rmt ORDER BY id;
    SELECT 'attached patch keeps its version', data_version = $version FROM system.parts
    WHERE database = currentDatabase() AND table = 't_rmt' AND active AND startsWith(name, 'patch-') AND rows > 0;
    DROP TABLE t_rmt SYNC;
"

echo "MOVE PART TO SHARD"
$CLIENT --query "
    CREATE TABLE t_src (id UInt64, a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_src', 'r1') ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, part_moves_between_shards_enable = 1, assign_part_uuids = 1;
    CREATE TABLE t_src_r2 (id UInt64, a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_src', 'r2') ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, part_moves_between_shards_enable = 1, assign_part_uuids = 1;
    CREATE TABLE t_dst (id UInt64, a UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_dst', 'r1') ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, part_moves_between_shards_enable = 1, assign_part_uuids = 1;
    INSERT INTO t_src VALUES (1, 0);
    SYSTEM SYNC REPLICA t_src_r2;
    SYSTEM STOP FETCHES t_src_r2;
    UPDATE t_src SET a = 5 WHERE id = 1;
"

patch=$($CLIENT --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_src' AND active AND startsWith(name, 'patch-')")
$CLIENT --query "ALTER TABLE t_src MOVE PART '$patch' TO SHARD '/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_dst'" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
part=$($CLIENT --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 't_src' AND active AND NOT startsWith(name, 'patch-')")
$CLIENT --query "ALTER TABLE t_src MOVE PART '$part' TO SHARD '/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_dst'" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'
# t_src_r2 has not fetched the patch, so the patches of the other replicas are checked as well.
$CLIENT --query "SELECT 'replica without the patch', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_src_r2' AND active AND startsWith(name, 'patch-')"
$CLIENT --query "ALTER TABLE t_src_r2 MOVE PART '$part' TO SHARD '/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_dst'" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'
# The patches keep their updates after a setting that lightweight updates require is disabled.
$CLIENT --query "ALTER TABLE t_src MODIFY SETTING enable_block_offset_column = 0"
$CLIENT --query "ALTER TABLE t_src MOVE PART '$part' TO SHARD '/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_dst'" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'
$CLIENT --query "
    SELECT id, a FROM t_src ORDER BY id;
    SELECT count() FROM system.part_moves_between_shards WHERE database = currentDatabase();
"
$CLIENT --query "KILL PART_MOVE_TO_SHARD WHERE database = currentDatabase()" > /dev/null
$CLIENT --query "
    SYSTEM START FETCHES t_src_r2;
    DROP TABLE t_src SYNC;
    DROP TABLE t_src_r2 SYNC;
    DROP TABLE t_dst SYNC;
"
