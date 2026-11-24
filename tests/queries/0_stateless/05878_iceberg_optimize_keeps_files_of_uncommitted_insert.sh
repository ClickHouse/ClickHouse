#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-parallel: the fail point is global, and the insert of another test could hit it.

# `OPTIMIZE` that rewrites the table between the upload of an insert and its commit must not remove the files of the
# insert, which are not referenced by the table metadata yet. The insert loses the commit race, retries on top of the
# compacted table, and its committed snapshot must reference files that still exist.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The cloud build runs `OPTIMIZE` through a different code path.
IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")
if [[ "${IS_CLOUD}" == "1" ]]; then
    cat "${CUR_DIR}/$(basename "${BASH_SOURCE[0]}" .sh).reference"
    exit 0
fi

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap '${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_writes_pause_before_commit" ||:; rm -rf "${TABLE_PATH}"' EXIT

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} SELECT number FROM numbers(10)"
# A position delete makes `OPTIMIZE` rewrite the data files.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --mutations_sync=2 --query "ALTER TABLE ${TABLE} DELETE WHERE id < 3"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_writes_pause_before_commit"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --iceberg_insert_merge_manifests=1 --query "INSERT INTO ${TABLE} VALUES (100)" &
insert_pid=$!
${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT iceberg_writes_pause_before_commit PAUSE"

${CLICKHOUSE_CLIENT} --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${TABLE}"

${CLICKHOUSE_CLIENT} --query "SYSTEM NOTIFY FAILPOINT iceberg_writes_pause_before_commit"
wait "${insert_pid}"

${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id) FROM ${TABLE}"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id) FROM icebergLocal('${TABLE_PATH}', 'Parquet')"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
