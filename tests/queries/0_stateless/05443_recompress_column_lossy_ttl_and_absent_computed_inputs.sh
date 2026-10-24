#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree
# no-fasttest: needs the SZ3 library
# no-shared-merge-tree: the test uses `SYSTEM STOP MERGES` to keep a mutation of a plain local
# MergeTree table queued and polls `system.mutations` of the single local server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Two more ways for a dependent of a column rewritten by a lossy `RECOMPRESS COLUMN` to keep the
# values computed before the recompression, which the execution-time guard must reject:
# - `MATERIALIZE TTL` in the same task computes a `MATERIALIZED` column the part does not store;
# - a stored `MATERIALIZED` column reads a computed column the part does not store, which reads
#   evaluate from the recompressed column.

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
        SELECT count() > 0, countIf(latest_fail_reason LIKE '%Code: 344%MATERIALIZED column \`$2\` is computed%') = count()
        FROM system.mutations
        WHERE database = currentDatabase() AND table = '$table' AND NOT is_done"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), countIf($3) FROM $table"
    ${CLICKHOUSE_CLIENT} --query "KILL MUTATION WHERE database = currentDatabase() AND table = '$table' SYNC FORMAT Null"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE $table"
}

# `MATERIALIZE TTL` resets the expired `y` and computes `m MATERIALIZED x + y`, which the part does not
# store, from `x` as it was before the recompression. Recompressing `y`, which uses the default codec,
# rewrites the whole part, including `x` with its lossy codec.
table="t_recompress_lossy_ttl"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $table"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE $table
    (
        key UInt64,
        ts DateTime,
        x Float64 CODEC(ZSTD(1)),
        y UInt64 TTL ts
    )
    ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             min_level_for_full_part_storage = 0, materialize_ttl_recalculate_only = 0,
             max_postpone_time_for_failed_mutations_ms = 100"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP TTL MERGES $table"
${CLICKHOUSE_CLIENT} --query "INSERT INTO $table SELECT number, toDateTime('2020-01-01 00:00:00'), sin(number / 100.) * 100, 1 FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $table ADD COLUMN m Float64 MATERIALIZED x + y"
${CLICKHOUSE_CLIENT} --enable_sz3_codec 1 --query "ALTER TABLE $table MODIFY COLUMN x CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01))"
${CLICKHOUSE_CLIENT} --mutations_sync 0 --query "ALTER TABLE $table MATERIALIZE TTL, RECOMPRESS COLUMN y"
wait_for_mutation "$table"
print_result "$table" "m" "m != x + y"

# `m2 MATERIALIZED m1 + 1` is stored, but `m1 MATERIALIZED x * 2` is not, so reads compute `m1` from `x`.
# Recompressing `x` lossily would make `m2` disagree with `m1`. The recompression is queued while `x`
# still has a lossless codec, so the check of the `ALTER` passes.
table="t_recompress_lossy_absent_input"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS $table"
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE $table
    (
        key UInt64,
        x Float64 CODEC(ZSTD(1))
    )
    ENGINE = MergeTree ORDER BY key
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
             max_postpone_time_for_failed_mutations_ms = 100"
${CLICKHOUSE_CLIENT} --query "INSERT INTO $table SELECT number, sin(number / 100.) * 100 FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $table ADD COLUMN m1 Float64 MATERIALIZED x * 2"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE $table ADD COLUMN m2 Float64 MATERIALIZED m1 + 1"
${CLICKHOUSE_CLIENT} --mutations_sync 2 --query "ALTER TABLE $table MATERIALIZE COLUMN m2"
${CLICKHOUSE_CLIENT} --query "SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = '$table' AND active AND column IN ('m1', 'm2') ORDER BY column"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES $table"
${CLICKHOUSE_CLIENT} --mutations_sync 0 --query "ALTER TABLE $table RECOMPRESS COLUMN x"
${CLICKHOUSE_CLIENT} --enable_sz3_codec 1 --query "ALTER TABLE $table MODIFY COLUMN x CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01))"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES $table"
wait_for_mutation "$table"
print_result "$table" "m2" "m2 != m1 + 1"
