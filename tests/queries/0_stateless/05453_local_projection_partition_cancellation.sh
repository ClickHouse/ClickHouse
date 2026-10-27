#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# no-parallel: both pause points affect merges across the server.
# no-fasttest: requires failpoints. The partition blocker under test belongs to `StorageMergeTree`.

set -euo pipefail
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

optimize_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_$$_optimize"
replace_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_$$_replace"
optimize_log="${CLICKHOUSE_TMP}/${optimize_id}.log"
replace_log="${CLICKHOUSE_TMP}/${replace_id}.log"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause;
        SYSTEM DISABLE FAILPOINT merge_task_pause_after_write;
        KILL QUERY WHERE query_id IN ('${optimize_id}', '${replace_id}') SYNC
            SETTINGS kill_throw_if_noop = 0 FORMAT Null;
        DROP TABLE IF EXISTS projection_cancel SYNC;
        DROP TABLE IF EXISTS projection_cancel_source SYNC;"
    wait
    rm -f "$optimize_log" "$replace_log"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE projection_cancel
        (partition UInt32, key UInt32, value UInt32, PROJECTION p (SELECT * ORDER BY value))
        ENGINE = MergeTree PARTITION BY partition ORDER BY key
        SETTINGS enable_vertical_merge_algorithm = 0,
            max_bytes_to_merge_at_max_space_in_pool = 1, max_bytes_to_merge_at_min_space_in_pool = 1;
    CREATE TABLE projection_cancel_source AS projection_cancel;
    INSERT INTO projection_cancel SELECT 7, number, number FROM numbers(10000);
    INSERT INTO projection_cancel SELECT 7, number + 10000, number FROM numbers(10000);
    SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause;"

# The size limits prevent background selection while allowing this synchronous merge.
timeout 60 ${CLICKHOUSE_CLIENT} --query_id "$optimize_id" \
    --query "OPTIMIZE TABLE projection_cancel FINAL" >"$optimize_log" 2>&1 &
optimize_pid=$!
timeout 30 ${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT merge_task_projection_stage_pause PAUSE"

# Base-part writes are complete. The cancelled child must not reach its first write.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT merge_task_pause_after_write"
timeout 60 ${CLICKHOUSE_CLIENT} --query_id "$replace_id" --query "
    ALTER TABLE projection_cancel REPLACE PARTITION 7 FROM projection_cancel_source
    SETTINGS allow_replace_partition_from_empty_source = 1" >"$replace_log" 2>&1 &
replace_pid=$!

# The query can appear in `system.processes` before it acquires the partition blocker.
# This message is emitted after acquisition, while the paused merge prevents completion.
blocked=0
for _ in {1..100}; do
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS text_log"
    if [[ $(${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM system.text_log WHERE query_id = '${replace_id}'
        AND message LIKE 'StorageMergeTree::stopMergesAndWaitForPartition Waiting%'"
        ) != 0 ]]; then
        blocked=1
        break
    fi
    sleep 0.1
done
if [[ "$blocked" != 1 ]]; then
    echo 'The replacement did not acquire the partition blocker'
    cat "$replace_log"
    exit 1
fi

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"
if ! wait "$replace_pid"; then
    cat "$replace_log"
    exit 1
fi
if wait "$optimize_pid"; then
    echo 'The merge unexpectedly succeeded'
    exit 1
fi
grep -q 'ABORTED' "$optimize_log"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM projection_cancel"
