#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest, no-replicated-database, no-shared-catalog, no-shared-merge-tree
# Tag no-parallel: replicated_queue_fail_next_entry is a server-global ONCE failpoint, so a concurrent
#                  test's queue entry would consume it instead of the fetch of the base part
# Tag no-shared-merge-tree, no-shared-catalog: tests the ReplicatedMergeTree queue and the leader's cleanup
# Tag no-replicated-database: the test drives individual replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

CLIENT="$CLICKHOUSE_CLIENT --insert_keeper_fault_injection_probability=0 --enable_lightweight_update=1"

function restore_failpoints()
{
    $CLICKHOUSE_CLIENT -q "system disable failpoint replicated_queue_fail_next_entry" ||:
    $CLICKHOUSE_CLIENT -q "system enable failpoint replicated_queue_unfail_entries" ||:
    $CLICKHOUSE_CLIENT -q "system start replication queues t_r1" ||:
    $CLICKHOUSE_CLIENT -q "system sync replica t_r1" ||:
    $CLICKHOUSE_CLIENT -q "system disable failpoint replicated_queue_unfail_entries" ||:
}
trap restore_failpoints EXIT

function wait_for()
{
    local query=$1
    for ((i = 0; i < 300; ++i)); do
        if [[ $($CLIENT -q "$query") == 1 ]]; then
            return 0
        fi
        sleep 0.1
    done
    echo "Timed out waiting for: $query" >&2
    exit 1
}

# Only t_r1 is a leader, so only t_r1 runs the cleanup that removes unused patch parts.
$CLIENT -m -q "
    drop table if exists t_r1 sync;
    drop table if exists t_r2 sync;

    create table t_r1 (p UInt8, id UInt64, val UInt64)
    engine = ReplicatedMergeTree('/clickhouse/tables/{database}/t_patch_before_base', 'r1')
    partition by p order by id
    settings enable_block_number_column = 1, enable_block_offset_column = 1, remove_unused_patch_parts = 1,
        cleanup_delay_period = 1, max_cleanup_delay_period = 1, cleanup_delay_period_random_add = 0,
        replicated_can_become_leader = 1;

    create table t_r2 (p UInt8, id UInt64, val UInt64)
    engine = ReplicatedMergeTree('/clickhouse/tables/{database}/t_patch_before_base', 'r2')
    partition by p order by id
    settings enable_block_number_column = 1, enable_block_offset_column = 1, remove_unused_patch_parts = 1,
        cleanup_delay_period = 1, max_cleanup_delay_period = 1, cleanup_delay_period_random_add = 0,
        replicated_can_become_leader = 0;

    insert into t_r2 values (2, 2, 200);
    system sync replica t_r1 strict;
    update t_r2 set val = 222 where id = 2;
    system sync replica t_r1 strict;
    system sync replica t_r2 strict;

    system stop replication queues t_r1;
    insert into t_r2 values (1, 1, 100);
    system sync replica t_r2 strict;
    system sync replica t_r1 pull;
"

# The fetch of the base part of partition 1 fails on t_r1 and stays failed.
$CLIENT -m -q "
    system enable failpoint replicated_queue_fail_next_entry;
    system start replication queues t_r1;
"
wait_for "select count() = 1 from system.replication_queue where database = currentDatabase() and table = 't_r1'
    and type = 'GET_PART' and startsWith(new_part_name, '1_') and num_tries > 0 and last_exception like '%FAULT_INJECTED%'"

# The patch part of partition 1 is fetched by t_r1 before the part it patches.
$CLIENT -q "update t_r2 set val = 999 where id = 1"
wait_for "system flush logs part_log; select count() = 1 from system.part_log where event_date >= yesterday()
    and database = currentDatabase() and table = 't_r1' and event_type = 'DownloadPart' and startsWith(part_name, 'patch-')
    and endsWith(partition_id, '-1')"

# Materialize the patch of partition 2: once t_r1 has dropped it, its cleanup has run with the patch of
# partition 1 present and the base part of partition 1 absent.
$CLIENT -q "alter table t_r1 apply patches in partition 2 settings mutations_sync = 2"
wait_for "select count() = 0 from system.parts where database = currentDatabase() and table = 't_r1'
    and active and startsWith(name, 'patch-') and endsWith(partition_id, '-2')"

echo -n 'fetch of the base part of partition 1 still fails on t_r1: '
$CLIENT -q "select count() = 1 from system.replication_queue where database = currentDatabase() and table = 't_r1'
    and type = 'GET_PART' and startsWith(new_part_name, '1_') and last_exception like '%FAULT_INJECTED%'"
echo -n 't_r1 has no regular part of partition 1: '
$CLIENT -q "select count() = 0 from system.parts where database = currentDatabase() and table = 't_r1'
    and active and partition_id = '1'"
echo -n 't_r1 keeps the patch part of partition 1: '
$CLIENT -q "select count() from system.parts where database = currentDatabase() and table = 't_r1'
    and active and startsWith(name, 'patch-') and endsWith(partition_id, '-1')"

restore_failpoints
trap '' EXIT

$CLIENT -m -q "
    system sync replica t_r1 strict;
    system sync replica t_r2 strict;
"

echo 'data on t_r1:'
$CLIENT -q "select p, id, val from t_r1 order by id"
echo 'data on t_r2:'
$CLIENT -q "select p, id, val from t_r2 order by id"

$CLIENT -m -q "
    drop table t_r1 sync;
    drop table t_r2 sync;
"
