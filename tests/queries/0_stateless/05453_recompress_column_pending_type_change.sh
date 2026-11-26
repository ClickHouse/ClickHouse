#!/usr/bin/env bash
# Tags: no-shared-merge-tree
# no-shared-merge-tree: the test uses `SYSTEM STOP MERGES` to keep a mutation of a plain local
# MergeTree table queued and polls `system.mutations` of the single local server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_recompress_column=1"

# A `MODIFY COLUMN` that changes a type and is queued after `ALTER TABLE ... RECOMPRESS COLUMN` is
# already in the table metadata when the recompression runs, but not in the part. `RECOMPRESS COLUMN`
# is a grouping barrier, so it runs alone and must handle the type change itself:
# 1. The target column must not be recompressed in place against its stored bytes of the old type
#    (`T64` cannot compress `Float64`), or the mutation fails forever and blocks the conversion.
# 2. The whole-part rewrite converts every column to its current type, so it must rebuild the skip
#    indices of a converted column, or they keep the bounds of the old type.

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
    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), max(latest_fail_reason) FROM system.mutations
        WHERE database = currentDatabase() AND table = '$table' AND NOT is_done"
}

# 1. In-place recompression of a column whose type is changed by a pending `MODIFY COLUMN`.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_recompress_type_codec (k UInt64, v Float64 CODEC(NONE))
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, max_postpone_time_for_failed_mutations_ms = 100"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_recompress_type_codec SELECT number, number FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_recompress_type_codec"
${CLICKHOUSE_CLIENT} --mutations_sync 0 --query "ALTER TABLE t_recompress_type_codec RECOMPRESS COLUMN v"
${CLICKHOUSE_CLIENT} --alter_sync 0 --mutations_sync 0 --query "ALTER TABLE t_recompress_type_codec MODIFY COLUMN v UInt64 CODEC(T64)"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES t_recompress_type_codec"
wait_for_mutations t_recompress_type_codec

${CLICKHOUSE_CLIENT} --query "
    SELECT type FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_recompress_type_codec' AND active AND column = 'v'"
${CLICKHOUSE_CLIENT} --query "SELECT sum(v) FROM t_recompress_type_codec"

# 2. A skip index on another column whose type is changed by a pending `MODIFY COLUMN`. The column
#    `x` inherits the table default codec, so the part is rewritten as a whole.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t_recompress_type_index (k UInt64, x UInt64, y Int64, INDEX iy y TYPE minmax GRANULARITY 1)
    ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, alter_column_secondary_index_mode = 'rebuild',
             packed_skip_index_max_bytes = 0, max_postpone_time_for_failed_mutations_ms = 100"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_recompress_type_index VALUES (1, 1, -1), (2, 2, 0), (3, 3, 1)"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES t_recompress_type_index"
${CLICKHOUSE_CLIENT} --mutations_sync 0 --query "ALTER TABLE t_recompress_type_index RECOMPRESS COLUMN x"
${CLICKHOUSE_CLIENT} --alter_sync 0 --mutations_sync 0 --query "ALTER TABLE t_recompress_type_index MODIFY COLUMN y UInt64"
${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES t_recompress_type_index"
wait_for_mutations t_recompress_type_index

${CLICKHOUSE_CLIENT} --query "
    SELECT type FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_recompress_type_index' AND active AND column = 'y'"
${CLICKHOUSE_CLIENT} --query "SELECT 'index', groupArray(k) FROM t_recompress_type_index WHERE y = 0 SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'iy'"
${CLICKHOUSE_CLIENT} --query "SELECT 'full scan', groupArray(k) FROM t_recompress_type_index WHERE y = 0 SETTINGS use_skip_indexes = 0"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_recompress_type_codec"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_recompress_type_index"
