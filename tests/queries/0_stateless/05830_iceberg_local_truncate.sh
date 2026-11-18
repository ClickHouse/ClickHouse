#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires `IcebergLocal` (USE_AVRO).

# `TRUNCATE` of an Iceberg table without a catalog adds one `DELETE` snapshot.
# The earlier snapshots stay readable, and the next writer builds on the truncate snapshot.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_truncate"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}" ||:
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

SETTINGS="allow_insert_into_iceberg = 1"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS ${SETTINGS} VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE ${TABLE} SETTINGS ${SETTINGS}"

echo "after truncate"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "
    SELECT operation FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
    ORDER BY toString(operation)"

echo "rows per snapshot"
for SNAPSHOT in $(${CLICKHOUSE_CLIENT} --query "SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${TABLE}'"); do
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE} SETTINGS iceberg_snapshot_id = ${SNAPSHOT}"
done | sort -n

echo "after insert"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS ${SETTINGS} VALUES (4)"
${CLICKHOUSE_CLIENT} --query "SELECT id FROM ${TABLE} ORDER BY id"
