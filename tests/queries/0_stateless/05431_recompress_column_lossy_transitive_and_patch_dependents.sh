#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree
# no-fasttest: needs the SZ3 library
# no-shared-merge-tree: the test uses `SYSTEM STOP MERGES` to keep a mutation of a plain local
# MergeTree table queued and polls `system.mutations` of the single local server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The execution-time lossy-codec guard of `ALTER TABLE ... RECOMPRESS COLUMN` skips a `MATERIALIZED`
# column the part does not store, unless the same task computes it. The task computes every
# `MATERIALIZED` column that reads a column it changes, transitively, and a column is also changed by
# materializing a patch part of a lightweight update. In both cases the dependent would be computed
# from the values before the lossy recompression, so the mutation must fail instead.

wait_for_mutation()
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

print_result()
{
    local table=$1
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count() > 0, countIf(latest_fail_reason LIKE '%Code: 344%MATERIALIZED column \`m\` is computed%') = count()
        FROM system.mutations
        WHERE database = currentDatabase() AND table = '$table' AND NOT is_done"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), countIf(m != $2) FROM $table"
    ${CLICKHOUSE_CLIENT} --query "KILL MUTATION WHERE database = currentDatabase() AND table = '$table' SYNC FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE $table"
}

# `y` -> `m1 MATERIALIZED y * 2` -> `m MATERIALIZED x + m1`: an `UPDATE` of `y` computes `m`, which the
# part does not store, from `x` as it was before the recompression. Recompressing `y`, which uses the
# default codec, rewrites the whole part, including `x` with its lossy codec.
table="t_recompress_lossy_transitive"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $table"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE $table
    (
        key UInt64,
        x Float64 CODEC(ZSTD(1)),
        y UInt64,
        m1 UInt64 MATERIALIZED y * 2
    )
    ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             max_postpone_time_for_failed_mutations_ms = 100"
${CLICKHOUSE_CLIENT} --query "INSERT INTO $table (key, x, y) SELECT number, sin(number / 100.) * 100, 0 FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $table ADD COLUMN m Float64 MATERIALIZED x + m1"
${CLICKHOUSE_CLIENT} --enable_sz3_codec 1 --query "ALTER TABLE $table MODIFY COLUMN x CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01))"
${CLICKHOUSE_CLIENT} --mutations_sync 0 --query "ALTER TABLE $table UPDATE y = y + 1 WHERE 1, RECOMPRESS COLUMN y"
wait_for_mutation "$table"
print_result "$table" "x + m1"

# The same through a patch part: materializing the lightweight update of `y` computes `m`.
table="t_recompress_lossy_patch"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $table"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE $table
    (
        key UInt64,
        x Float64 CODEC(ZSTD(1)),
        y UInt64
    )
    ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             enable_block_number_column = 1, enable_block_offset_column = 1,
             max_postpone_time_for_failed_mutations_ms = 100"
${CLICKHOUSE_CLIENT} --query "INSERT INTO $table (key, x, y) SELECT number, sin(number / 100.) * 100, 0 FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --enable_sz3_codec 1 --query "ALTER TABLE $table MODIFY COLUMN x CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01))"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES $table"
${CLICKHOUSE_CLIENT} --enable_lightweight_update 1 --query "UPDATE $table SET y = y + 1 WHERE 1"
${CLICKHOUSE_CLIENT} --mutations_sync 0 --query "ALTER TABLE $table RECOMPRESS COLUMN x"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $table ADD COLUMN m Float64 MATERIALIZED x + y"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES $table"
wait_for_mutation "$table"
print_result "$table" "x + y"
