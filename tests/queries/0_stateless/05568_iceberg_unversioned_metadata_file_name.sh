#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg (Avro) is not in fast test

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
M="${TABLE_PATH}metadata"

function cleanup()
{
    rm -rf "${TABLE_PATH}" 2>/dev/null
}
trap cleanup EXIT

CLIENT="${CLICKHOUSE_CLIENT} --iceberg_metadata_staleness_ms=0 --allow_insert_into_iceberg=1"

function sum_with()
{
    ${CLIENT} --query "SELECT sum(c0) FROM icebergLocal('${TABLE_PATH}'${1:+, SETTINGS $1})" 2>&1
}

${CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int64) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLIENT} --query "INSERT INTO ${TABLE} VALUES (1)"
${CLIENT} --query "INSERT INTO ${TABLE} VALUES (2)"

# Names other writers use, each holding the state of v2 (sum 1); vfinal is the newest by last-updated-ms.
for name in vfinal no-snapshot-schema-id spec_id_999 v99999999999999999999; do
    cp "${M}/v2.metadata.json" "${M}/${name}.metadata.json"
done
sed -i -E 's/"last-updated-ms" : [0-9]+/"last-updated-ms" : 9999999999999/' "${M}/vfinal.metadata.json"

for name in vfinal no-snapshot-schema-id spec_id_999; do
    echo "explicit ${name}: $(sum_with "iceberg_metadata_file_path = 'metadata/${name}.metadata.json'")"
done
echo "listing by version: $(sum_with)"
echo "listing by last-updated-ms: $(sum_with "iceberg_recent_metadata_file_by_last_updated_ms_field = 1")"
UUID=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["table-uuid"])' "${M}/v3.metadata.json")
echo "listing by table uuid: $(sum_with "iceberg_metadata_table_uuid = '${UUID}'")"

echo -n "vfinal.metadata.json" > "${M}/version-hint.text"
echo "version hint: $(sum_with "iceberg_use_version_hint = 1")"
rm "${M}/version-hint.text"

${CLIENT} --query "INSERT INTO ${TABLE} VALUES (10)" 2>&1 | grep -q "whose name has no version number" && echo "insert refused" || echo "insert not refused"
echo "metadata files: $(ls "${M}"/*.metadata.json | wc -l)"
echo "listing by version: $(sum_with)"

rm "${M}/v1.metadata.json" "${M}/v2.metadata.json" "${M}/v3.metadata.json" "${M}/v99999999999999999999.metadata.json"
sum_with | grep -q "cannot be found by its version number.*(FILE_DOESNT_EXIST)" && echo "only unversioned, listing by version: error" || echo "only unversioned, listing by version: no error"
echo "only unversioned, listing by last-updated-ms: $(sum_with "iceberg_recent_metadata_file_by_last_updated_ms_field = 1")"
echo "only unversioned, explicit: $(sum_with "iceberg_metadata_file_path = 'metadata/vfinal.metadata.json'")"

${CLIENT} --query "DROP TABLE ${TABLE}"
