#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro.

# `system.iceberg_history` must return a snapshot whose summary counters exceed INT32_MAX.
# https://github.com/ClickHouse/ClickHouse/issues/94176

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./iceberg.lib
. "$CUR_DIR"/iceberg.lib

ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${ROOT:?}"
trap 'rm -rf "${ROOT:?}"' EXIT

for fv in 1 2; do
    echo "--- format version ${fv}"
    TABLE="t_fv${fv}"
    TABLE_PATH="${ROOT}/${TABLE}/"

    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
        CREATE TABLE ${TABLE} (x Int) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version = ${fv};
        INSERT INTO ${TABLE} VALUES (1);"

    # The counter from the bug report, above INT32_MAX.
    iceberg_edit_metadata "${TABLE_PATH}metadata" \
        '.snapshots[].summary |= . + {"added-data-files": $huge, "added-records": $huge, "added-files-size": $huge, "changed-partition-count": $huge}' \
        --arg huge 6986350573 > /dev/null

    ${CLICKHOUSE_CLIENT} --query "
        SELECT count(), any(summary['added-records']) FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${TABLE}';
        DROP TABLE ${TABLE} SYNC;"
done
