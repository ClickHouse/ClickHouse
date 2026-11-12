#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# An `INSERT` copies the entries of the manifest list of the parent snapshot into the manifest list that it writes, and
# accepts a field of an entry only if its Avro type is the one that the Iceberg specification declares: `manifest_path` a
# `string`, `manifest_length` a `long`, `partition_spec_id` an `int`. Another Avro type is rejected, also one that reads
# as the same ClickHouse type (`bytes` and `string` both read as `String`) or converts to it (`int` to `Int64`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1"
TABLE="t_${CLICKHOUSE_DATABASE}_parent_list_field_kinds"
TABLE_PATH="${USER_FILES_PATH:?}/${TABLE}"

$CH -q "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}/', 'Parquet') SETTINGS allow_experimental_iceberg_compaction = 0"
$CH -q "INSERT INTO ${TABLE} VALUES (1)"

PARENT_LIST=$(echo "${TABLE_PATH}"/metadata/snap-*.avro)
SNAPSHOT_ID=$($CH -q "SELECT added_snapshot_id FROM file('${PARENT_LIST}', 'Avro')")

# Replaces the parent list by one with 3 entries. The manifests they name do not exist: the `INSERT` only copies them.
# $1: the type of `manifest_path`, $2: of `manifest_length`, $3: of `partition_spec_id`, $4: the value of the setting
# `output_format_avro_string_column_pattern`: the `String` columns that the writer writes as an Avro `string`, all of
# them for an empty pattern, the other ones it writes as `bytes`.
write_parent_list()
{
    $CH -q "
        INSERT INTO FUNCTION file('${PARENT_LIST}', 'Avro',
            'manifest_path $1, manifest_length $2, partition_spec_id $3, content Int32, sequence_number Int64,
             min_sequence_number Int64, added_snapshot_id Int64, added_files_count Int32, existing_files_count Int32,
             deleted_files_count Int32, added_rows_count Int64, existing_rows_count Int64, deleted_rows_count Int64')
        SETTINGS engine_file_truncate_on_insert = 1, output_format_avro_string_column_pattern = '$4'
        SELECT concat('metadata/manifest-', toString(number), '.avro'), 1000 + number, 0, 0, 1, 1,
            ${SNAPSHOT_ID}, 1, 0, 0, 1, 0, 0
        FROM numbers(3)"
}

# Prints the code of the exception that the `INSERT` fails with, or `inserted`, and then the number of snapshots of the table.
insert()
{
    local out
    if out=$($CH -q "INSERT INTO ${TABLE} VALUES (2)" 2>&1); then
        echo inserted
    elif [[ "${out}" =~ \(([A-Z_]+)\) ]]; then
        echo "${BASH_REMATCH[1]}"
    else
        echo "${out}"
    fi
    $CH -q "SELECT count() FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${TABLE}'"
}

echo "manifest_path bytes"
write_parent_list String Int64 Int32 'no_column_is_named_like_this'
insert

echo "manifest_length int"
write_parent_list String Int32 Int32 ''
insert

echo "partition_spec_id long"
write_parent_list String Int64 Int64 ''
insert

echo "the types of the specification"
write_parent_list String Int64 Int32 ''
insert

$CH -q "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
