#!/usr/bin/env bash

# Parts written by older versions with `enable_block_offset_column = 1` and `enable_block_number_column = 0`
# store `_block_offset` without `_block_number`. `_block_number` reads as the constant `min_block`, so a part
# merged from several insert blocks has duplicate `(_block_number, _block_offset)` pairs.
#
# 1. The stored `_block_offset` of such a part must be ignored and read as the row position in the part,
#    so lightweight updates after `MODIFY SETTING enable_block_number_column = 1` change only the matched rows.
#    A mutation that materializes `_block_number` must rewrite `_block_offset` too.
# 2. A part that stores both columns with duplicate pairs can't be detected on load. A patch that matches
#    more than one row of it must throw instead of changing all of them.
#
# The current version can't write such parts, so the test edits the files of a merged part.
# It uses `clickhouse local` with its own directory for each table, and does not touch the data of the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP}/05837_lwu_block_offset_without_block_number_stored"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

# The first argument is the table: each table has its own `clickhouse local` directory.
function local_query()
{
    ${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}/$1" --multiquery -q "$2" </dev/null 2>&1
}

TABLE_SETTINGS="enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0,
    min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, ratio_of_defaults_for_sparse_serialization = 1"

# Rows `a` and `c` are in block 1, row `b` is in block 2. After the merge, the part stores
# `_block_number` = [1, 2, 1] and `_block_offset` = [0, 0, 1] in the order of `uid`: `a`, `b`, `c`.
function create_table()
{
    local_query "$1" "
        CREATE TABLE $1 (uid Int16, v String) ENGINE = MergeTree ORDER BY uid
        SETTINGS ${TABLE_SETTINGS}, patch_parts_version = '$2';
        INSERT INTO $1 VALUES (0, 'a'), (1, 'c');
        INSERT INTO $1 VALUES (0, 'b');
        OPTIMIZE TABLE $1 FINAL;
    "
}

# The directory of the part `$2` of the table `$1`.
function part_dir()
{
    find "${WORKING_DIR}/$1" -type d -name "$2"
}

# Removes the stored `_block_number` of the part. Missing `checksums.txt` is calculated again when the part is loaded.
function remove_block_number()
{
    local dir
    dir=$(part_dir "$1" all_1_2_1)
    rm "${dir}"/_block_number.* "${dir}/checksums.txt"
    sed -i -e '/^`_block_number` UInt64$/d' -e 's/^4 columns:$/3 columns:/' "${dir}/columns.txt"
    sed -i -e '/^1 substreams for column `_block_number`:$/,+1d' -e 's/^4 columns:$/3 columns:/' "${dir}/columns_substreams.txt"
    sed -i -e 's/{[^}]*"name":"_block_number"[^}]*},//' "${dir}/serialization.json"
}

echo "stored _block_offset without _block_number"

for version in v1 v2
do
    echo "${version}"
    create_table t_lwu_${version} ${version}
    remove_block_number t_lwu_${version}

    local_query t_lwu_${version} "
        SET enable_lightweight_update = 1, apply_patch_parts = 1, mutations_sync = 2;
        ALTER TABLE t_lwu_${version} MODIFY SETTING enable_block_number_column = 0;

        SELECT column FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_lwu_${version}' AND active AND NOT startsWith(name, 'patch-') AND column IN ('_block_number', '_block_offset') ORDER BY column;

        -- The stored offsets are ignored: the offsets are the row positions, and the pairs are unique.
        SELECT _block_number, _block_offset, v FROM t_lwu_${version} ORDER BY v;

        ALTER TABLE t_lwu_${version} MODIFY SETTING enable_block_number_column = 1;
        UPDATE t_lwu_${version} SET v = 'X' WHERE v = 'a';
        SELECT v FROM t_lwu_${version} ORDER BY v;

        -- The part is rewritten, the patch is applied in Join mode for v1.
        OPTIMIZE TABLE t_lwu_${version} FINAL;
        SELECT v FROM t_lwu_${version} ORDER BY v;

        -- The merge stored both columns with unique pairs.
        SELECT column FROM system.parts_columns
        WHERE database = currentDatabase() AND table = 't_lwu_${version}' AND active AND NOT startsWith(name, 'patch-') AND column IN ('_block_number', '_block_offset') ORDER BY column;
        SELECT count() = uniqExact(_block_number, _block_offset) FROM t_lwu_${version};
    "
done

echo "mutation"

create_table t_mutation v2
remove_block_number t_mutation

local_query t_mutation "
    SET enable_lightweight_update = 1, apply_patch_parts = 1, mutations_sync = 2;
    ALTER TABLE t_mutation MODIFY SETTING enable_block_number_column = 1;

    -- The mutation materializes _block_number and must rewrite _block_offset too.
    ALTER TABLE t_mutation UPDATE v = concat(v, '') WHERE 1;
    SELECT column FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_mutation' AND active AND NOT startsWith(name, 'patch-') AND column IN ('_block_number', '_block_offset') ORDER BY column;
    SELECT _block_number, _block_offset, v FROM t_mutation ORDER BY v;

    UPDATE t_mutation SET v = 'X' WHERE v = 'a';
    SELECT v FROM t_mutation ORDER BY v;
"

echo "both columns stored with duplicate pairs"

# A table with the same three rows in one insert block. Its merged part stores `_block_number` = [1, 1, 1].
local_query t_one_block "
    CREATE TABLE t_one_block (uid Int16, v String) ENGINE = MergeTree ORDER BY uid SETTINGS ${TABLE_SETTINGS};
    INSERT INTO t_one_block VALUES (0, 'a'), (0, 'b'), (1, 'c');
    OPTIMIZE TABLE t_one_block FINAL;
"
ONE_BLOCK_DIR=$(part_dir t_one_block all_1_1_1)

for version in v1 v2
do
    echo "${version}"
    create_table t_dup_${version} ${version}

    # Now the part stores `_block_number` = [1, 1, 1] and `_block_offset` = [0, 0, 1]: `a` and `b` are both (1, 0).
    DIR=$(part_dir t_dup_${version} all_1_2_1)
    cp "${ONE_BLOCK_DIR}"/_block_number.* "${DIR}/"
    rm "${DIR}/checksums.txt"

    local_query t_dup_${version} "
        SET enable_lightweight_update = 1, apply_patch_parts = 1, mutations_sync = 2;
        SELECT _block_number, _block_offset, v FROM t_dup_${version} ORDER BY v;

        -- The row c is unique.
        UPDATE t_dup_${version} SET v = 'Y' WHERE v = 'c';
        SELECT v FROM t_dup_${version} ORDER BY v;
    "

    # A patch for the row a also matches the row b. For v1, the patch is applied in Merge mode by
    # (_part, _part_offset) and is exact until the part is rewritten. Then Join mode must throw.
    local_query t_dup_${version} "
        SET enable_lightweight_update = 1, apply_patch_parts = 1, mutations_sync = 2;
        UPDATE t_dup_${version} SET v = 'X' WHERE v = 'a';
        OPTIMIZE TABLE t_dup_${version} FINAL;
        SELECT v FROM t_dup_${version} ORDER BY v;
    " | grep -o -m1 -e 'INCORRECT_DATA' -e '^X$'

    # The data can still be read without patches.
    local_query t_dup_${version} "SELECT v FROM t_dup_${version} ORDER BY v SETTINGS apply_patch_parts = 0"
done

rm -rf "${WORKING_DIR}"
