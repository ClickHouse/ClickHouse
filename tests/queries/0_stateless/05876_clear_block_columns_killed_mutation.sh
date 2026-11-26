#!/usr/bin/env bash
# Tags: zookeeper, no-parallel
# no-parallel: the test arms server-wide failpoints.

# A part mutated by `CLEAR COLUMN _block_number` can still be committed after its mutation is killed, so lightweight
# updates stay rejected until the running mutation of the part finishes. Otherwise an update would store the values
# of `_block_number` and `_block_offset` of the source part, which the committed part does not have.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

trap '
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_before_rename_part" 2>/dev/null || true
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT rmt_mutate_task_pause_before_rename_part" 2>/dev/null || true
' EXIT

function run()
{
    local engine=$1
    local failpoint=$2

    echo "$engine"

    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS t_clear_block_kill;
        CREATE TABLE t_clear_block_kill (id UInt64, v UInt64) ENGINE = $engine ORDER BY id
        SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
        INSERT INTO t_clear_block_kill SETTINGS async_insert = 0 VALUES (1, 1);
        INSERT INTO t_clear_block_kill SETTINGS async_insert = 0 VALUES (2, 2);
        OPTIMIZE TABLE t_clear_block_kill FINAL;"

    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $failpoint"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE t_clear_block_kill CLEAR COLUMN _block_number SETTINGS alter_sync = 0"
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $failpoint PAUSE"

    $CLICKHOUSE_CLIENT -q "KILL MUTATION WHERE database = currentDatabase() AND table = 't_clear_block_kill' FORMAT Null"

    $CLICKHOUSE_CLIENT -q "UPDATE t_clear_block_kill SET v = 7 WHERE id = 2 SETTINGS enable_lightweight_update = 1" 2>&1 \
        | grep -o -m1 "SUPPORT_IS_DISABLED"

    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $failpoint"

    # The update is accepted once the mutation of the part has finished.
    for _ in {1..600}; do
        $CLICKHOUSE_CLIENT -q "UPDATE t_clear_block_kill SET v = 7 WHERE id = 2 SETTINGS enable_lightweight_update = 1" 2>/dev/null && break
        sleep 0.1
    done

    $CLICKHOUSE_CLIENT -q "
        SELECT id, v, _block_offset FROM t_clear_block_kill ORDER BY id;
        DROP TABLE t_clear_block_kill;"
}

run "MergeTree" "mt_mutate_task_pause_before_rename_part"
run "ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_block_kill', 'r1')" "rmt_mutate_task_pause_before_rename_part"
