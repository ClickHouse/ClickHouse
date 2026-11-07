#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires `IcebergLocal` (USE_AVRO).
# - no-replicated-database: `IcebergLocal` is non-replicated.

# Iceberg `timestamp_ns` is an Avro `long`. A scale-9 `DateTime64` used to be written as
# decimal `bytes`, so the data file disagreed with the table metadata.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}/t_ice/"
PLAIN_AVRO="${CLICKHOUSE_USER_FILES_UNIQUE}/plain.avro"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
mkdir -p "${TABLE_PATH}"
trap 'rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"' EXIT

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
CREATE TABLE t (ts DateTime64(9))
ENGINE = IcebergLocal('${TABLE_PATH}', 'Avro')
SETTINGS iceberg_format_version = 3
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "
INSERT INTO t VALUES ('2020-01-02 03:04:05.123456789')
"

${CLICKHOUSE_CLIENT} --query "SELECT ts FROM t"

DATA_FILE=$(find "${TABLE_PATH}/data" -name 'data-*.avro' -type f | head -1)
if [ -z "${DATA_FILE}" ]; then
    echo "Iceberg Avro data file not found"
    exit 1
fi
RELATIVE_DATA_FILE=${DATA_FILE#"${CLICKHOUSE_USER_FILES}/"}

# The data file is a plain Avro `long`, which schema inference reads as `Int64`.
${CLICKHOUSE_CLIENT} --query "SELECT toTypeName(ts) FROM file('${RELATIVE_DATA_FILE}', 'Avro')"

# Generic Avro output is unchanged: scale 9 stays a decimal.
RELATIVE_PLAIN_AVRO=${PLAIN_AVRO#"${CLICKHOUSE_USER_FILES}/"}
${CLICKHOUSE_CLIENT} --query "
INSERT INTO FUNCTION file('${RELATIVE_PLAIN_AVRO}', 'Avro')
SELECT toDateTime64('2020-01-02 03:04:05.123456789', 9) AS ts
"
${CLICKHOUSE_CLIENT} --query "
SELECT if(toTypeName(ts) LIKE 'Decimal%', 'Decimal', toTypeName(ts))
FROM file('${RELATIVE_PLAIN_AVRO}', 'Avro')
"
