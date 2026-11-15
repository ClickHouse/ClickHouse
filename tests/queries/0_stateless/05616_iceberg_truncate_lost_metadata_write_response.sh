#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires `IcebergLocal` (USE_AVRO).

# `TRUNCATE` of an Iceberg table without a catalog commits by a conditional PUT of the next metadata file.
# When that PUT is applied but reported as failed (e.g. its response is lost), the truncate must recognise
# the metadata file as its own instead of deleting the manifest list it references: every snapshot in the
# history must stay readable, and the truncate must not be committed twice.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}_truncate_lost_response"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
FP="iceberg_truncate_lose_metadata_write_response"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FP}" ||:
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}" ||:
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

SETTINGS="allow_insert_into_iceberg = 1"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS ${SETTINGS} VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

# The metadata file is written, but the truncate is told that the write failed.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${FP}"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE ${TABLE} SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FP}"

echo "after truncate"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

# Exactly one truncate snapshot: the one whose write was reported as failed is the committed one.
${CLICKHOUSE_CLIENT} --query "
    SELECT operation FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TABLE}'
    ORDER BY toString(operation)"

# Every snapshot must be readable: none may reference a deleted manifest list.
echo "rows per snapshot"
for SNAPSHOT in $(${CLICKHOUSE_CLIENT} --query "SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${TABLE}'"); do
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE} SETTINGS iceberg_snapshot_id = ${SNAPSHOT}"
done | sort -n

# The next writer builds on the truncate snapshot.
echo "after insert"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS ${SETTINGS} VALUES (4)"
${CLICKHOUSE_CLIENT} --query "SELECT id FROM ${TABLE} ORDER BY id"
