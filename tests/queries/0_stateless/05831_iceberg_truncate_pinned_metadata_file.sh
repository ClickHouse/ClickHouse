#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires `IcebergLocal` (USE_AVRO).

# A table without a catalog that is pinned with `iceberg_metadata_file_path` never reads a newer metadata file.
# `TRUNCATE` of such a table is refused, because the table itself would not see the result.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LIVE_TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_live"
PINNED_TABLE="${LIVE_TABLE}_pinned"
TABLE_PATH="${USER_FILES_PATH}/${LIVE_TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${PINNED_TABLE}" ||:
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${LIVE_TABLE}" ||:
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

SETTINGS="allow_insert_into_iceberg = 1"

# `CREATE` writes `metadata/v1.metadata.json` with no rows. The `INSERT` writes `metadata/v2.metadata.json`.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${LIVE_TABLE} (x Int32) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${LIVE_TABLE} SETTINGS ${SETTINGS} VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${LIVE_TABLE}"

# A second table over the same directory, pinned to the empty first version.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE IF NOT EXISTS ${PINNED_TABLE} (x Int32) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    SETTINGS iceberg_metadata_file_path = 'metadata/v1.metadata.json'"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${PINNED_TABLE}"

ERROR=$(${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE ${PINNED_TABLE} SETTINGS ${SETTINGS}" 2>&1 ||:)
echo "${ERROR}" | grep -q "BAD_ARGUMENTS" && echo "BAD_ARGUMENTS"
echo "${ERROR}" | grep -q "iceberg_metadata_file_path" && echo "iceberg_metadata_file_path"

# The unpinned table over the same directory can still be truncated.
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE ${LIVE_TABLE} SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${LIVE_TABLE}"
