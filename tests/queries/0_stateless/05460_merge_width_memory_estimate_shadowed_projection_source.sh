#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A vertical merge is priced by the columns it merges on the horizontal stage, which include the columns of the
# projections the merge rebuilds. A merge rebuilds a projection that reads a source column named like a subcolumn
# of another column which the parts miss (`a.size0` next to an `Array` column `a`), whatever
# `deduplicate_merge_projection_mode` is, even when every source part has the projection.
#
# Such a column can no longer be added by `ALTER TABLE ... ADD COLUMN`, so the table of an older version is simulated
# by editing its metadata offline. The table then has 16 columns, a single key column, and an aggregate projection
# over 15 columns. The estimate is sized from the memory limit so that a merge that merges 16 columns on the horizontal
# stage affords two parts, while a vertical merge priced by the key alone affords 32 parts. The memory limit of
# `clickhouse-local` follows the memory of the host, which may change between its runs, so the test checks only that
# `OPTIMIZE` does not merge all eight parts at once.

WORKING_FOLDER="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

clickhouse_local()
{
    ${CLICKHOUSE_LOCAL} --path="${WORKING_FOLDER}" --query "$1"
}

MEMORY_LIMIT=$(clickhouse_local "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'")
# The same successive integer divisions as the server does: `limit / 16 / columns / estimate`.
ESTIMATE=$(( MEMORY_LIMIT / 16 / 16 / 2 ))

for mode in rebuild ignore
do
    # `merge_selector_base = 1000` is far above what eight fresh parts reach, so background merges do not take
    # the parts, and `OPTIMIZE`, which selects with the base of one, merges them synchronously. The estimate is set
    # only after the projection is materialized, as the mutation that materializes it is priced by it as well.
    clickhouse_local "
        CREATE DATABASE IF NOT EXISTS d ENGINE = Atomic;
        CREATE TABLE d.t_$mode (id UInt64, a Array(UInt64),
            c1 UInt64, c2 UInt64, c3 UInt64, c4 UInt64, c5 UInt64, c6 UInt64, c7 UInt64,
            c8 UInt64, c9 UInt64, c10 UInt64, c11 UInt64, c12 UInt64, c13 UInt64, c14 UInt64)
        ENGINE = MergeTree ORDER BY id
        SETTINGS min_parts_to_merge_at_once = 2,
            merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once = 0,
            merge_selector_algorithm = 'Simple',
            merge_selector_base = 1000,
            min_bytes_for_wide_part = 0,
            min_rows_for_wide_part = 0,
            min_bytes_for_full_part_storage = 0,
            min_rows_for_full_part_storage = 0,
            allow_vertical_merges_from_compact_to_wide_parts = 1,
            vertical_merge_algorithm_min_columns_to_activate = 1,
            enable_vertical_merge_algorithm = 1,
            vertical_merge_algorithm_min_rows_to_activate = 1,
            vertical_merge_algorithm_min_bytes_to_activate = 0,
            enable_block_number_column = 0,
            enable_block_offset_column = 0,
            deduplicate_merge_projection_mode = '$mode';
        INSERT INTO d.t_$mode (id, a) VALUES (0, [0]);
        INSERT INTO d.t_$mode (id, a) VALUES (1, [1]);
        INSERT INTO d.t_$mode (id, a) VALUES (2, [2]);
        INSERT INTO d.t_$mode (id, a) VALUES (3, [3]);
        INSERT INTO d.t_$mode (id, a) VALUES (4, [4]);
        INSERT INTO d.t_$mode (id, a) VALUES (5, [5]);
        INSERT INTO d.t_$mode (id, a) VALUES (6, [6]);
        INSERT INTO d.t_$mode (id, a) VALUES (7, [7]);
        ALTER TABLE d.t_$mode ADD PROJECTION p
            (SELECT sum(\`a.size0\` + c1 + c2 + c3 + c4 + c5 + c6 + c7 + c8 + c9 + c10 + c11 + c12 + c13 + c14));
        ALTER TABLE d.t_$mode MATERIALIZE PROJECTION p SETTINGS mutations_sync = 1;
        ALTER TABLE d.t_$mode MODIFY SETTING merge_memory_estimate_per_source_part_column = $ESTIMATE;"

    # shellcheck disable=SC2016
    sed -i 's/^    `a` Array(UInt64),$/    `a` Array(UInt64),\n    `a.size0` UInt64 DEFAULT 7,/' "${WORKING_FOLDER}/metadata/d/t_$mode.sql"

    clickhouse_local "
        SELECT '$mode';
        SELECT 'before', count() FROM system.parts WHERE database = 'd' AND table = 't_$mode' AND active;
        OPTIMIZE TABLE d.t_$mode;
        SELECT 'after', count() > 1 FROM system.parts WHERE database = 'd' AND table = 't_$mode' AND active;
        SELECT sum(\`a.size0\`) FROM d.t_$mode SETTINGS optimize_use_projections = 0;"
done

rm -rf "${WORKING_FOLDER}"
