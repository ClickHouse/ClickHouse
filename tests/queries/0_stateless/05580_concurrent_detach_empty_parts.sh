#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# The pauseable failpoints are server-global.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

first_pid=""
second_pid=""
cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_outdating_detached_empty_parts"
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_after_detach_operations_lock"
    [[ -z "$first_pid" ]] || wait "$first_pid" || true
    [[ -z "$second_pid" ]] || wait "$second_pid" || true
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_concurrent_empty_parts SYNC"
}
trap cleanup EXIT

for first_command in "PARTITION 1" "PART '1_1_1_0'"; do
    $CLICKHOUSE_CLIENT --multiquery -q "
        CREATE TABLE t_concurrent_empty_parts (p UInt8, k UInt32)
        ENGINE = MergeTree PARTITION BY p ORDER BY k SETTINGS remove_empty_parts = 0;
        INSERT INTO t_concurrent_empty_parts SETTINGS async_insert = 0 VALUES (1, 1), (1, 2), (1, 3);
        SYSTEM ENABLE FAILPOINT mt_pause_before_outdating_detached_empty_parts;
    "
    $CLICKHOUSE_CLIENT -q "ALTER TABLE t_concurrent_empty_parts DETACH ${first_command}" &
    first_pid=$!
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_pause_before_outdating_detached_empty_parts PAUSE"
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_pause_after_detach_operations_lock"
    second_query="${CLICKHOUSE_DATABASE}_second_detach"
    $CLICKHOUSE_CLIENT --query_id "$second_query" -q "ALTER TABLE t_concurrent_empty_parts DETACH PARTITION 1" &
    second_pid=$!

    state=""
    for _ in {1..100}; do
        state=$($CLICKHOUSE_CLIENT -q "
            SELECT multiIf(
                countIf(arrayExists(x -> addressToSymbol(x) LIKE '%pauseFailPoint%', trace)) > 0, 'unlocked',
                countIf(arrayExists(x -> addressToSymbol(x) ILIKE '%mutex%lock%', trace)) > 0, 'locked',
                '')
            FROM system.stack_trace
            WHERE thread_id IN (SELECT arrayJoin(thread_ids) FROM system.processes WHERE query_id = '$second_query')
                AND query_id = '$second_query'
            SETTINGS allow_introspection_functions = 1")
        [[ -z "$state" ]] || break
        sleep 0.1
    done
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_after_detach_operations_lock"
    if [[ "$state" == "unlocked" ]]; then
        wait "$second_pid"
        second_pid=""
    fi
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_pause_before_outdating_detached_empty_parts"
    wait "$first_pid"
    first_pid=""
    if [[ -n "$second_pid" ]]; then
        wait "$second_pid"
        second_pid=""
    fi
    echo "Second detach: $state"
    $CLICKHOUSE_CLIENT -q "ALTER TABLE t_concurrent_empty_parts ATTACH PARTITION 1"
    $CLICKHOUSE_CLIENT -q "SELECT count(), sum(k) FROM t_concurrent_empty_parts"
    $CLICKHOUSE_CLIENT -q "DROP TABLE t_concurrent_empty_parts SYNC"
done
