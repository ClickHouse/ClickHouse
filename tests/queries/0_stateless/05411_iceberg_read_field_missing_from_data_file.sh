#!/usr/bin/env bash
# Tags: no-fasttest

# An INSERT planned before ADD COLUMN commits an Avro data file without the added column under the widened
# schema. Iceberg reads a column whose field id is not in a data file as NULL, also with
# `input_format_avro_allow_missing_fields` disabled.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${TABLE_DIR}"

LOCAL="${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --async_insert=0 --input_format_avro_allow_missing_fields=0"

TABLE_PATH="${TABLE_DIR}/"
mkdir -p "${TABLE_PATH}"
${LOCAL} --query "
    CREATE TABLE t (a Int32, c Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Avro');
    INSERT INTO t SELECT number, number FROM numbers(3);
"
PIN="metadata/$(basename "$(ls "${TABLE_PATH}metadata"/*.metadata.json | sort -V | tail -1)")"
${LOCAL} --query "
    CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Avro');
    ALTER TABLE t ADD COLUMN d Nullable(Int64);
"
# The structure comes from the version before ADD COLUMN, the commit goes to the widened table.
${LOCAL} --query "
    CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Avro') SETTINGS iceberg_metadata_file_path = '${PIN}';
    INSERT INTO t SELECT number + 10, number + 10 FROM numbers(3);
"
${LOCAL} --query "
    CREATE TABLE t ENGINE = IcebergLocal('${TABLE_PATH}', 'Avro');
    SELECT 'rows', arraySort(groupArray(a)), countIf(d IS NULL) FROM t;
    SELECT 'where_null', count() FROM t WHERE d IS NULL;
    SELECT 'where_eq', count() FROM t WHERE d = 1;
    SELECT 'star', * FROM t ORDER BY a;
"

rm -rf "${TABLE_DIR}"
