#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regressions for the MongoDB shell `ISODate("...")` wrapper in JSON input.

# A declared `DateTime64` column that the query does not read, or a `count()`, must not make the input unreadable.
DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.json"
COLUMNS_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}_columns.json"
echo '{"id":1,"ts":ISODate("2024-05-29T23:16:12.256Z")}' > "${USER_FILES_PATH}/${DATA_FILE}"
echo '{"ts":[ISODate("2024-05-29T23:16:12.256Z")]}' > "${USER_FILES_PATH}/${COLUMNS_FILE}"

$CLICKHOUSE_CLIENT --session_timezone UTC -q "SELECT * FROM file('${DATA_FILE}', JSONEachRow, 'id UInt8, ts DateTime64(3)')"
$CLICKHOUSE_CLIENT -q "SELECT id FROM file('${DATA_FILE}', JSONEachRow, 'id UInt8, ts DateTime64(3)')"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM file('${DATA_FILE}', JSONEachRow, 'id UInt8, ts DateTime64(3)')"
$CLICKHOUSE_CLIENT --session_timezone UTC -q "SELECT * FROM file('${COLUMNS_FILE}', JSONColumns, 'ts DateTime64(3)')"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM file('${COLUMNS_FILE}', JSONColumns, 'ts DateTime64(3)')"

rm -f "${USER_FILES_PATH}/${DATA_FILE}" "${USER_FILES_PATH}/${COLUMNS_FILE}"

# A wrapper in a `Variant` must not disable reading sibling objects and arrays as strings.
$CLICKHOUSE_CLIENT --session_timezone UTC -q "SELECT v FROM format(JSONEachRow, 'v Variant(Tuple(ts DateTime64(3), payload String), UInt64)', '{\"v\":{\"ts\":ISODate(\"2024-05-29T23:16:12.256Z\"),\"payload\":{\"x\":1}}}') SETTINGS input_format_json_read_objects_as_strings = 1"
$CLICKHOUSE_CLIENT --session_timezone UTC -q "SELECT v FROM format(JSONEachRow, 'v Variant(Tuple(ts DateTime64(3), payload String), UInt64)', '{\"v\":{\"ts\":ISODate(\"2024-05-29T23:16:12.256Z\"),\"payload\":[1,2]}}') SETTINGS input_format_json_read_arrays_as_strings = 1"
# But a `String` still does not take an object containing the wrapper.
$CLICKHOUSE_CLIENT -q "SELECT v FROM format(JSONEachRow, 'v Variant(Tuple(ts DateTime64(3), payload String), UInt64)', '{\"v\":{\"ts\":ISODate(\"2024-05-29T23:16:12.256Z\"),\"payload\":{\"x\":ISODate(\"2024-05-29T23:16:12.256Z\")}}}')" 2>&1 | grep -o -m1 'INCORRECT_DATA'

# `null` and `new ISODate` share a prefix: malformed tokens must not be accepted as `null`.
$CLICKHOUSE_CLIENT -q "SELECT v FROM format(JSONEachRow, 'v Variant(DateTime64(3), String)', '{\"v\":{\"x\":neull}}')" 2>&1 | grep -o -m1 'CANNOT_PARSE_INPUT_ASSERTION_FAILED'
$CLICKHOUSE_CLIENT -q "SELECT v FROM format(JSONEachRow, 'v Variant(DateTime64(3), String)', '{\"v\":{\"x\":new ISODateull}}')" 2>&1 | grep -o -m1 'CANNOT_PARSE_INPUT_ASSERTION_FAILED'
$CLICKHOUSE_CLIENT -q "SELECT v, variantType(v) FROM format(JSONEachRow, 'v Variant(DateTime64(3), String)', '{\"v\":{\"x\":null}}')"
