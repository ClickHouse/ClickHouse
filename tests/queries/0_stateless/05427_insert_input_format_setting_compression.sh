#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# `input` can take its format from `SETTINGS input_format` instead of a trailing `FORMAT`.
# `COMPRESSION` must work with this form in `clickhouse-client` and in `clickhouse-local`.

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"
LOCAL_PATH="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_local"

printf '1,A\n2,B\n' > "${DATA_FILE}"
gzip -k -f "${DATA_FILE}"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE test_input_format_setting_compression (id UInt32, text String) ENGINE = Memory"

echo "client, explicit gzip"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_input_format_setting_compression COMPRESSION 'gzip' SELECT * FROM input('id UInt32, text String') SETTINGS input_format = 'CSV'" < "${DATA_FILE}.gz"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_input_format_setting_compression ORDER BY id"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE test_input_format_setting_compression"

echo "client, auto"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test_input_format_setting_compression COMPRESSION 'auto' SELECT * FROM input('id UInt32, text String') SETTINGS input_format = 'CSV'" < "${DATA_FILE}.gz"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM test_input_format_setting_compression ORDER BY id"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test_input_format_setting_compression"

echo "local, explicit gzip"
rm -rf "${LOCAL_PATH}"
mkdir -p "${LOCAL_PATH}"
${CLICKHOUSE_LOCAL} --path "${LOCAL_PATH}" --query "
CREATE TABLE test_input_format_setting_compression (id UInt32, text String) ENGINE = MergeTree ORDER BY id;
INSERT INTO test_input_format_setting_compression COMPRESSION 'gzip' SELECT * FROM input('id UInt32, text String') SETTINGS input_format = 'CSV'
" < "${DATA_FILE}.gz"
${CLICKHOUSE_LOCAL} --path "${LOCAL_PATH}" --query "SELECT * FROM test_input_format_setting_compression ORDER BY id"

rm -f "${DATA_FILE}" "${DATA_FILE}.gz"
rm -rf "${LOCAL_PATH}"
