#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP:?}/into_outfile_append_${CLICKHOUSE_DATABASE:?}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"

echo '-- JSON, non-empty file: the second export is refused and the file is unchanged'
${CLICKHOUSE_LOCAL} -q "SELECT toUInt32(number) AS id FROM numbers(3) INTO OUTFILE '${WORK_DIR}/o.json' FORMAT JSON"
cp "${WORK_DIR}/o.json" "${WORK_DIR}/o.json.orig"
${CLICKHOUSE_LOCAL} -q "SELECT toUInt32(number + 10) AS id FROM numbers(3) INTO OUTFILE '${WORK_DIR}/o.json' APPEND FORMAT JSON" 2>&1 \
    | grep -oF 'CANNOT_APPEND_TO_FILE' | head -n 1
${CLICKHOUSE_CLIENT} -q "SELECT toUInt32(number + 10) AS id FROM numbers(3) INTO OUTFILE '${WORK_DIR}/o.json' APPEND FORMAT JSON" 2>&1 \
    | grep -oF 'CANNOT_APPEND_TO_FILE' | head -n 1
cmp -s "${WORK_DIR}/o.json" "${WORK_DIR}/o.json.orig" && echo 'unchanged'

echo '-- JSON, existing empty file: written as usual'
: > "${WORK_DIR}/empty.json"
${CLICKHOUSE_LOCAL} -q "SELECT toUInt32(number) AS id FROM numbers(3) INTO OUTFILE '${WORK_DIR}/empty.json' APPEND FORMAT JSON"
${CLICKHOUSE_LOCAL} -q "SELECT groupArray(id) FROM file('${WORK_DIR}/empty.json', JSON, 'id UInt32')"

echo '-- CSVWithNames, non-empty file: rows are appended without a second header'
${CLICKHOUSE_LOCAL} -q "SELECT toUInt32(number) AS id FROM numbers(3) INTO OUTFILE '${WORK_DIR}/o.csv' FORMAT CSVWithNames"
${CLICKHOUSE_LOCAL} -q "SELECT toUInt32(number + 10) AS id FROM numbers(3) INTO OUTFILE '${WORK_DIR}/o.csv' APPEND FORMAT CSVWithNames"
cat "${WORK_DIR}/o.csv"

rm -rf "${WORK_DIR}"
