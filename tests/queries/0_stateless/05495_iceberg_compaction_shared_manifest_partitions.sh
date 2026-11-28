#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --iceberg_insert_single_manifest_for_all_partitions=1"

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (p Int64, n Int64)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    PARTITION BY p"

# One insert puts the files of all three partitions into one manifest; compaction must keep each file's own partition.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} SELECT number % 3, number FROM numbers(9)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query "ALTER TABLE ${TABLE} DELETE WHERE n = 4"

# Stderr is captured, because the cloud build does not run this compaction.
optimize_err=$(${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE}" 2>&1)
if [[ "${IS_CLOUD}" != "1" && -n "${optimize_err}" ]]; then
    echo "OPTIMIZE failed: ${optimize_err}"
fi

${CLICKHOUSE_CLIENT} --query "
    SELECT p, groupArray(n) FROM (SELECT p, n FROM ${TABLE} ORDER BY n) GROUP BY p ORDER BY p"
${CLICKHOUSE_CLIENT} --query "
    SELECT partition FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = '${TABLE}' AND content = 'DATA'
    ORDER BY partition"
# A file stamped with another partition's value is pruned away by these filters.
for p in 0 1 2; do
    ${CLICKHOUSE_CLIENT} --query "SELECT ${p}, groupArray(n) FROM (SELECT n FROM ${TABLE} WHERE p = ${p} ORDER BY n)"
done
