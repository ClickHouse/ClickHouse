#!/usr/bin/env bash
# Tags: no-fasttest
# `ALTER TABLE ... DROP PARTITION` must record the previous metadata file in the metadata log of the new one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_t0"

rm -rf "${TABLE_PATH}"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t0 (a Int64, b String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY (a)
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO t0 VALUES (1, 'x')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO t0 VALUES (2, 'y')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE t0 DROP PARTITION 1"

echo '--- metadata files and their snapshots ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT
        extract(file, 'v(\\\\d+)[^/]*\\\\.metadata\\\\.json$') AS version,
        latest_sequence_number
    FROM system.iceberg_metadata_log_entries
    WHERE database = currentDatabase() AND table = 't0'
    ORDER BY timestamp, file
"

echo '--- the newest metadata file points at the DROP PARTITION snapshot ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT latest_snapshot_id = (
        SELECT snapshot_id FROM system.iceberg_history
        WHERE database = currentDatabase() AND table = 't0' AND operation = 'DELETE')
    FROM system.iceberg_metadata_log_entries
    WHERE database = currentDatabase() AND table = 't0'
    ORDER BY timestamp DESC, file DESC
    LIMIT 1
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t0"
rm -rf "${TABLE_PATH}"
