#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro.

# Format v1 Iceberg snapshots omit `sequence-number` and default to 0 per the Iceberg specification:
# `system.iceberg_metadata_log_entries.latest_sequence_number` must report 0 (matching `system.iceberg_files`),
# while entries written before any snapshot still report NULL.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${ROOT:?}"
trap 'rm -rf "${ROOT:?}"' EXIT

TABLE="t_iceberg_v1_05782"
TABLE_PATH="${ROOT}/${TABLE}/"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    DROP TABLE IF EXISTS ${TABLE};
    CREATE TABLE ${TABLE} (id Int32) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version = 1;
"

echo "--- empty format v1 table ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT latest_snapshot_id IS NULL, latest_schema_id, latest_sequence_number
    FROM system.iceberg_metadata_log_entries
    WHERE database = currentDatabase() AND table = '${TABLE}'
    ORDER BY timestamp, file;
"

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE} VALUES (1);
    INSERT INTO ${TABLE} VALUES (2);
"

echo "--- after two inserts ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT latest_snapshot_id IS NULL, latest_schema_id, latest_sequence_number
    FROM system.iceberg_metadata_log_entries
    WHERE database = currentDatabase() AND table = '${TABLE}'
    ORDER BY timestamp, file;
"

echo "--- matches system.iceberg_files ---"
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), countIf(sequence_number = 0)
    FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = '${TABLE}';
    DROP TABLE ${TABLE} SYNC;
"
