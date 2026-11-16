#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Appending to a non-empty file skips the RowDescription prefix: the file must equal one export of all rows.

WORK_DIR="${CLICKHOUSE_TMP:?}/pgwire_append_${CLICKHOUSE_DATABASE:?}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"

echo '-- INTO OUTFILE APPEND'
${CLICKHOUSE_CLIENT} -q "SELECT number AS x, range(number) AS a FROM numbers(3) INTO OUTFILE '${WORK_DIR}/appended.bin' FORMAT PostgreSQLWire"
${CLICKHOUSE_CLIENT} -q "SELECT number + 3 AS x, range(number + 3) AS a FROM numbers(3) INTO OUTFILE '${WORK_DIR}/appended.bin' APPEND FORMAT PostgreSQLWire"
${CLICKHOUSE_CLIENT} -q "SELECT number AS x, range(number) AS a FROM numbers(6) INTO OUTFILE '${WORK_DIR}/single.bin' FORMAT PostgreSQLWire"
cmp -s "${WORK_DIR}/appended.bin" "${WORK_DIR}/single.bin" && echo 'equal'

echo '-- INSERT INTO FUNCTION file'
FILE_DIR="${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
STRUCTURE='x UInt64, a Array(UInt8)'
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION file('${FILE_DIR}/appended.bin', 'PostgreSQLWire', '${STRUCTURE}') SELECT number, range(number) FROM numbers(3) SETTINGS engine_file_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION file('${FILE_DIR}/appended.bin', 'PostgreSQLWire', '${STRUCTURE}') SELECT number + 3, range(number + 3) FROM numbers(3)"
${CLICKHOUSE_CLIENT} -q "INSERT INTO FUNCTION file('${FILE_DIR}/single.bin', 'PostgreSQLWire', '${STRUCTURE}') SELECT number, range(number) FROM numbers(6) SETTINGS engine_file_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} -q "SELECT (SELECT raw_blob FROM file('${FILE_DIR}/appended.bin', 'RawBLOB')) = (SELECT raw_blob FROM file('${FILE_DIR}/single.bin', 'RawBLOB'))"

rm -rf "${WORK_DIR}" "${CLICKHOUSE_USER_FILES_UNIQUE:?}"
