#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro.

# A commit compresses its metadata file. When the compressing buffer cannot be created, the buffer of the storage is
# still alive when the commit reads the outcome of the write from it. Nothing is published, so the commit fails with
# that error and removes its data file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${ROOT:?}"
trap 'rm -rf "${ROOT:?}"' EXIT

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t (x Int64) ENGINE = IcebergLocal('${ROOT}', 'Parquet') SETTINGS iceberg_metadata_compression_method = 'gzip';
    INSERT INTO t VALUES (1);"

# The table compresses its metadata with gzip, and the commit takes the level from the query. gzip has no level 18.
${CLICKHOUSE_CLIENT} --send_logs_level=fatal --allow_insert_into_iceberg=1 --query "
    INSERT INTO t SETTINGS output_format_compression_level = 18, output_format_parquet_compression_method = 'none' VALUES (2)" 2>&1 \
    | grep -oE 'Compression level 18 is out of range for gzip.*\([A-Z_]+\)$'

${CLICKHOUSE_CLIENT} --query "SELECT * FROM t ORDER BY ALL"
find "${ROOT}" -name '*.parquet' | wc -l
${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
