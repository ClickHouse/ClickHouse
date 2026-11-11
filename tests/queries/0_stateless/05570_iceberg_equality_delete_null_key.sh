#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-replicated-database: IcebergLocal is non-replicated.

# Iceberg equality deletes compare keys null-safely: "A null value in a delete column matches a row
# if the row's value is null, equivalent to col IS NULL". Rows with a NULL key must survive a delete
# file without NULLs, and a NULL in a delete file must delete only NULL keys.
# The fixture is described in data_minio/deletes_db/README.md (`eq_deletes_null_key_table`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAKE_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}/eq_deletes_null_key_table"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
cp -r "${CUR_DIR}/data_minio/deletes_db/eq_deletes_null_key_table" "${LAKE_DIR}"

${CLICKHOUSE_CLIENT} --query "SELECT id, category, name, tag FROM icebergLocal('${LAKE_DIR}/') ORDER BY name"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM icebergLocal('${LAKE_DIR}/')"
${CLICKHOUSE_CLIENT} --query "SELECT name FROM icebergLocal('${LAKE_DIR}/') WHERE id IS NULL ORDER BY name"

# The delete file keeps its NULLs whatever the schema inference settings are.
${CLICKHOUSE_CLIENT} --query "SELECT name FROM icebergLocal('${LAKE_DIR}/') ORDER BY name SETTINGS schema_inference_make_columns_nullable = 0"
${CLICKHOUSE_CLIENT} --query "SELECT name FROM icebergLocal('${LAKE_DIR}/') ORDER BY name SETTINGS schema_inference_make_columns_nullable = 0, input_format_null_as_default = 0"

rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
