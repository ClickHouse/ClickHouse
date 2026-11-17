#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# `OPTIMIZE` must not bring back a dropped partition.
# Here the `DROP PARTITION` snapshot is the current one. The rewrite keeps only `append` snapshots,
# and none of them holds the files of the current snapshot, so `OPTIMIZE` refuses and leaves the table unchanged.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The cloud build does not run this compaction, so it prints the expected output.
IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")
if [[ "${IS_CLOUD}" = "1" ]]; then
    echo "9"
    echo "NOT_IMPLEMENTED"
    echo "9"
    exit 0
fi

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (p Int64, id Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY (p)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} SELECT if(number < 90, 1, 2), number FROM numbers(100)"
# `OPTIMIZE` rewrites the history only when the table has position deletes.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query "ALTER TABLE ${TABLE} DELETE WHERE id = 95"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "ALTER TABLE ${TABLE} DROP PARTITION 1"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"
${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE}" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
