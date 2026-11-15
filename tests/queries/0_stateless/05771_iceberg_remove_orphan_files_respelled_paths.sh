#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires IcebergLocal (USE_AVRO build option)
# - no-replicated-database: `ALTER TABLE ... EXECUTE` runs as replicated DDL there and returns no result rows

# `remove_orphan_files` on local storage keeps the referenced files spelled with `//` or `./`, or relative to `user_files`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./iceberg_orphan_files.lib
. "$CUR_DIR"/iceberg_orphan_files.lib

echo "--- the table path ends with //"
populate local trailing_slashes 2
add_orphan data/orphan-slashes.parquet
$CH -q "CREATE TABLE ${TABLE}_slashes ENGINE = IcebergLocal('${DIR}//', 'Parquet')"
OUT=$($CH -q "ALTER TABLE ${TABLE}_slashes EXECUTE remove_orphan_files(older_than = '$(now_ts)')")
metric deleted_data_files_count
metric deleted_manifest_files_count
metric deleted_manifest_lists_count
exists data/orphan-slashes.parquet
data
$CH -q "DROP TABLE ${TABLE}_slashes"

echo "--- the manifest lists are spelled with ./"
populate local dot_manifest_lists 2
n=$(find "${META}" -name 'v*.metadata.json' | sed 's|.*/v\([0-9]*\)\.metadata\.json$|\1|' | sort -n | tail -1)
sed 's|/metadata/snap-|/./metadata/snap-|g' "${META}/v${n}.metadata.json" > "${META}/v$((n + 1)).metadata.json"
add_orphan data/orphan-dot.parquet
remove_orphans "older_than = '$(now_ts)'"
metric deleted_data_files_count
metric deleted_manifest_lists_count
exists data/orphan-dot.parquet
data

echo "--- the table path is relative to user_files"
populate local relative 2
add_orphan data/orphan-relative.parquet
$CH -q "CREATE TABLE ${TABLE}_relative ENGINE = IcebergLocal('${CLICKHOUSE_TEST_UNIQUE_NAME}/${TABLE}/', 'Parquet')"
OUT=$($CH -q "ALTER TABLE ${TABLE}_relative EXECUTE remove_orphan_files(older_than = '$(now_ts)')")
metric deleted_data_files_count
metric deleted_manifest_files_count
metric deleted_manifest_lists_count
exists data/orphan-relative.parquet
data
$CH -q "DROP TABLE ${TABLE}_relative"
