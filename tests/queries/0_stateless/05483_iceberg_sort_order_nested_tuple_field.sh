#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# An Iceberg table sorted by a tuple element: the sort field's `source-id` must be the element's
# field id, INSERT must sort by it, a sort column that is not a valid Iceberg sort source is refused,
# and a table whose sort field names an unknown `source-id` (written by older versions) stays usable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_DIR="${USER_FILES_PATH}/t_${CLICKHOUSE_DATABASE}"
PFX="t_${CLICKHOUSE_DATABASE}"
CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0"

cleanup() {
    ${CH} -q "DROP TABLE IF EXISTS ${PFX}_nested; DROP TABLE IF EXISTS ${PFX}_deep; DROP TABLE IF EXISTS ${PFX}_size0;
        DROP TABLE IF EXISTS ${PFX}_element; DROP TABLE IF EXISTS ${PFX}_struct; DROP TABLE IF EXISTS ${PFX}_legacy;" > /dev/null 2>&1
    rm -rf "${BASE_DIR}"
}
cleanup
trap cleanup EXIT

# The sort field's `source-id` and the schema id of the field it must point to.
sort_source_id() {
    ${CH} -q "SELECT JSONExtractInt(json, 'sort-orders', 1, 'fields', 1, 'source-id'), JSONExtractInt(json, $2)
        FROM file('${BASE_DIR}/$1/metadata/v1.metadata.json', JSONAsString, 'json String')"
}

# Row order inside the only data file: proves the INSERT sorted the rows.
data_file_order() {
    ${CH} -q "SELECT groupArray(x) FROM file('${BASE_DIR}/$1/data/*.parquet', Parquet) SETTINGS max_threads = 1"
}

echo "-- tuple element"
${CH} -q "CREATE TABLE ${PFX}_nested (x Int32, t Tuple(a Int32, b String)) ENGINE = IcebergLocal('${BASE_DIR}/nested/') ORDER BY t.a"
sort_source_id nested "'schemas', 1, 'fields', 2, 'type', 'fields', 1, 'id'"
${CH} -q "INSERT INTO ${PFX}_nested VALUES (1, (3, 'c')), (2, (1, 'a')), (3, (2, 'b'))"
${CH} -q "SELECT x, t.a, t.b FROM ${PFX}_nested ORDER BY x"
data_file_order nested

echo "-- element of a nested tuple, under a transform"
${CH} -q "CREATE TABLE ${PFX}_deep (x Int32, t Tuple(u Tuple(s String))) ENGINE = IcebergLocal('${BASE_DIR}/deep/') ORDER BY icebergTruncate(1, t.u.s)"
sort_source_id deep "'schemas', 1, 'fields', 2, 'type', 'fields', 1, 'type', 'fields', 1, 'id'"
${CH} -q "INSERT INTO ${PFX}_deep VALUES (1, tuple(tuple('c'))), (2, tuple(tuple('a'))), (3, tuple(tuple('b')))"
data_file_order deep

echo "-- not a sort source"
for spec in "size0|x Int32, arr Array(Int32)|arr.size0" \
            "element|x Int32, arr Array(Tuple(element Int32))|arr.element" \
            "struct|x Int32, t Tuple(u Tuple(a Int32))|t.u"; do
    IFS='|' read -r tag columns key <<< "${spec}"
    ${CH} -q "CREATE TABLE ${PFX}_${tag} (${columns}) ENGINE = IcebergLocal('${BASE_DIR}/${tag}/') ORDER BY ${key}" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
    ${CH} -q "EXISTS TABLE ${PFX}_${tag}"
done

echo "-- unknown source-id"
${CH} -q "CREATE TABLE ${PFX}_legacy (x Int32, t Tuple(a Int32)) ENGINE = IcebergLocal('${BASE_DIR}/legacy/') ORDER BY x"
python3 -c "
import json, sys
path = sys.argv[1]
metadata = json.load(open(path))
metadata['sort-orders'][0]['fields'][0]['source-id'] = 0
json.dump(metadata, open(path, 'w'))
" "${BASE_DIR}/legacy/metadata/v1.metadata.json"
${CH} -q "SELECT x FROM icebergLocal('${BASE_DIR}/legacy/', 'Parquet') ORDER BY x"
${CH} -q "INSERT INTO ${PFX}_legacy VALUES (2, tuple(1)), (1, tuple(2))"
${CH} -q "SELECT x FROM icebergLocal('${BASE_DIR}/legacy/', 'Parquet') ORDER BY x"
