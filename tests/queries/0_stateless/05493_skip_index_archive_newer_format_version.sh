#!/usr/bin/env bash

# A part whose skip index archive skp_idx.packed was written in a newer, unknown format version
# (as after a rollback to an older server) stays readable, mergeable and mutable; only its skip indices are not used.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

data_path="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_newer_archive"
rm -rf "$data_path"
trap 'rm -rf "$data_path"' EXIT

# Creates a table with one part that has skp_idx.packed and replaces the archive's format version (its first byte) with 0xff.
function create_part_with_newer_archive()
{
    rm -rf "$data_path"
    ${CLICKHOUSE_LOCAL} --path "$data_path" --query "
        CREATE DATABASE d ENGINE = Atomic;
        CREATE TABLE d.t (id UInt64, v UInt64, INDEX i v TYPE minmax GRANULARITY 1)
        ENGINE = MergeTree ORDER BY id
        SETTINGS packed_skip_index_max_bytes = '1M', index_granularity = 100, $1
                 min_bytes_for_full_part_storage = 0, min_rows_for_full_part_storage = 0,
                 min_level_for_full_part_storage = 0;
        INSERT INTO d.t SELECT number, number FROM numbers(1000);
    "
    archives=("$data_path"/store/*/*/all_1_1_0/skp_idx.packed)
    test -f "${archives[0]}" || { echo "no skp_idx.packed"; exit 1; }
    original_version=$(head -c 1 "${archives[0]}" | od -An -tx1 | tr -d ' \n')
    printf '\xff' | dd of="${archives[0]}" bs=1 count=1 conv=notrunc 2>/dev/null
}

echo "read and merge"
create_part_with_newer_archive ""
${CLICKHOUSE_LOCAL} --path "$data_path" --query "
    SELECT count() FROM d.t;
    SELECT id FROM d.t WHERE v = 500;
    INSERT INTO d.t SELECT number, number FROM numbers(1000, 10);
    OPTIMIZE TABLE d.t FINAL;
    SELECT count() FROM d.t WHERE v = 500 SETTINGS force_data_skipping_indices = 'i';
"

# The mutated part must not keep the old archive: after an upgrade it would be readable again but describe the old values.
echo "mutation, then upgrade"
create_part_with_newer_archive "min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,"
${CLICKHOUSE_LOCAL} --path "$data_path" --query "ALTER TABLE d.t UPDATE v = 1000000 WHERE id = 500 SETTINGS mutations_sync = 2"
for archive in "$data_path"/store/*/*/all_*/skp_idx.packed; do
    test -f "$archive" && printf "\\x${original_version}" | dd of="$archive" bs=1 count=1 conv=notrunc 2>/dev/null
done
${CLICKHOUSE_LOCAL} --path "$data_path" --query "
    SELECT count() FROM d.t WHERE v = 1000000;
    SELECT count() FROM d.t WHERE v = 1000000 SETTINGS use_skip_indexes = 0;
"
