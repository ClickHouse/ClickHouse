#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# The pauseable failpoints are server-global and require FIU support.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

detach_pid=""
cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_outdating_detached_empty_parts"
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT storage_merge_tree_background_clear_old_parts_pause"
    [[ -z "$detach_pid" ]] || wait "$detach_pid" || true
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_detach_deleting_parts SYNC"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --multiquery -q "
    CREATE TABLE t_detach_deleting_parts (k UInt32)
    ENGINE = MergeTree ORDER BY k
    SETTINGS remove_empty_parts = 0, old_parts_lifetime = 0,
        merge_tree_clear_old_parts_interval_seconds = 1,
        cleanup_delay_period = 1, cleanup_delay_period_random_add = 0,
        max_cleanup_delay_period = 1;
    SYSTEM STOP CLEANUP t_detach_deleting_parts;
    INSERT INTO t_detach_deleting_parts SETTINGS async_insert = 0 VALUES (1), (2), (3);
    SYSTEM ENABLE FAILPOINT mt_pause_before_outdating_detached_empty_parts;
    SYSTEM ENABLE FAILPOINT storage_merge_tree_background_clear_old_parts_pause;
"

$CLICKHOUSE_CLIENT -q "ALTER TABLE t_detach_deleting_parts DETACH PARTITION tuple()" &
detach_pid=$!
timeout 30s $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_pause_before_outdating_detached_empty_parts PAUSE"
$CLICKHOUSE_CLIENT -q "SYSTEM START CLEANUP t_detach_deleting_parts"
timeout 30s $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT storage_merge_tree_background_clear_old_parts_pause PAUSE"
$CLICKHOUSE_CLIENT -q "
    SELECT name, _state FROM system.parts
    WHERE database = currentDatabase() AND table = 't_detach_deleting_parts'
    ORDER BY name"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_outdating_detached_empty_parts"
wait "$detach_pid"
detach_pid=""
$CLICKHOUSE_CLIENT -q "
    SELECT name, _state FROM system.parts
    WHERE database = currentDatabase() AND table = 't_detach_deleting_parts'
    ORDER BY name"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(k) FROM t_detach_deleting_parts"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT storage_merge_tree_background_clear_old_parts_pause"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_detach_deleting_parts ATTACH PARTITION tuple()"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(k) FROM t_detach_deleting_parts"
