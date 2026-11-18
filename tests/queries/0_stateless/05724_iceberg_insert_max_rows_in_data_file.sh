#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# iceberg_insert_max_rows_in_data_file = 1 starts a new data file only between insert blocks:
# 1058449 rows parsed into two blocks (max_insert_block_size = 1048449) give exactly two Parquet files,
# and every row reads back, for both format versions.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${ROOT}"
mkdir -p "${ROOT}"
trap 'rm -rf "${ROOT}"' EXIT

ROWS=1058449

for fv in 1 2; do
    echo "--- format version ${fv}"
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_v${fv} (x Int32) ENGINE = IcebergLocal('${ROOT}/t_v${fv}/') SETTINGS iceberg_format_version = ${fv}"
    # The block boundaries decide the file count, so pin the setting that shapes the insert blocks.
    seq -s ', ' -f '(%.0f)' 0 $((ROWS - 1)) | sed "s/^/INSERT INTO t_v${fv} VALUES /" > "${ROOT}/insert.sql"
    ${CLICKHOUSE_CLIENT} \
        --allow_insert_into_iceberg 1 \
        --iceberg_insert_max_rows_in_data_file 1 \
        --max_insert_block_size 1048449 \
        --queries-file "${ROOT}/insert.sql"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x), uniqExact(x) FROM t_v${fv}"
    find "${ROOT}/t_v${fv}/" -name '*.parquet' | wc -l
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE t_v${fv}"
done
