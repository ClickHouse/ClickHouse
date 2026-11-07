#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree
# no-fasttest: needs the SZ3 library
# no-shared-merge-tree: the test uses `SYSTEM STOP MERGES` to keep a mutation of a plain local
# MergeTree table queued and polls `system.mutations` of the single local server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_recompress_column=1"

# A lossy `RECOMPRESS COLUMN x` queued before a skip index on `x + y` is added and before a pending
# `MODIFY COLUMN` changes the type of `y`. The part does not have the index, but the whole-part rewrite
# converts `y` to its current type, and the interpreter rebuilds the index of the converted column -
# from the values of `x` as they were before the recompression. No command of the task names the index,
# so the guard must take the indices the interpreter actually builds and reject the recompression,
# instead of leaving the index inconsistent with the base column.

wait_for_mutations()
{
    local table=$1
    for _ in {1..600}
    do
        pending=$(${CLICKHOUSE_CLIENT} --query "
            SELECT count() FROM system.mutations
            WHERE database = currentDatabase() AND table = '$table' AND NOT is_done AND latest_fail_reason = ''")
        if [[ "$pending" == "0" ]]; then break; fi
        sleep 0.3
    done
}

table=t_recompress_lossy_index_type_conversion

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE $table (k UInt64, x Float64 CODEC(NONE), y Int64)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
             alter_column_secondary_index_mode = 'rebuild', packed_skip_index_max_bytes = 0, index_granularity = 100,
             max_postpone_time_for_failed_mutations_ms = 100"
${CLICKHOUSE_CLIENT} --query "INSERT INTO $table SELECT number, sin(number / 100.) * 100, 0 FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES $table"
${CLICKHOUSE_CLIENT} --mutations_sync 0 --query "ALTER TABLE $table RECOMPRESS COLUMN x"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $table ADD INDEX idx (x + y) TYPE set(10000) GRANULARITY 1"
${CLICKHOUSE_CLIENT} --enable_sz3_codec 1 --query "ALTER TABLE $table MODIFY COLUMN x CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01))"
${CLICKHOUSE_CLIENT} --alter_sync 0 --mutations_sync 0 --query "ALTER TABLE $table MODIFY COLUMN y UInt64"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES $table"

# The recompression is rejected because of the index, and the type conversion waits behind it.
for _ in {1..600}
do
    failed=$(${CLICKHOUSE_CLIENT} --query "
        SELECT count() FROM system.mutations
        WHERE database = currentDatabase() AND table = '$table' AND NOT is_done AND latest_fail_reason != ''")
    if [[ "$failed" != "0" ]]; then break; fi
    sleep 0.3
done
${CLICKHOUSE_CLIENT} --query "
    SELECT 'rejected', count(), countIf(latest_fail_reason LIKE '%index \`idx\` depends on this column%' AND latest_fail_reason LIKE '%SUPPORT_IS_DISABLED%') FROM system.mutations
    WHERE database = currentDatabase() AND table = '$table' AND NOT is_done AND command LIKE '%RECOMPRESS COLUMN%'"

# Dropping the index unblocks the queue.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $table DROP INDEX idx"
wait_for_mutations $table
${CLICKHOUSE_CLIENT} --query "
    SELECT 'unfinished', count() FROM system.mutations
    WHERE database = currentDatabase() AND table = '$table' AND NOT is_done"
${CLICKHOUSE_CLIENT} --query "
    SELECT type FROM system.parts_columns
    WHERE database = currentDatabase() AND table = '$table' AND active AND column = 'y'"

# Re-adding and materializing the index builds it from the recompressed values.
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $table ADD INDEX idx (x + y) TYPE set(10000) GRANULARITY 1"
${CLICKHOUSE_CLIENT} --mutations_sync 2 --query "ALTER TABLE $table MATERIALIZE INDEX idx"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE expected ENGINE = Memory AS SELECT x + y AS v FROM $table"
${CLICKHOUSE_CLIENT} --query "SELECT 'full scan', count() FROM $table WHERE x + y IN (SELECT v FROM expected) SETTINGS use_skip_indexes = 0"
${CLICKHOUSE_CLIENT} --query "SELECT 'index', count() FROM $table WHERE x + y IN (SELECT v FROM expected) SETTINGS use_skip_indexes = 1, force_data_skipping_indices = 'idx'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE expected"
${CLICKHOUSE_CLIENT} --query "DROP TABLE $table"
