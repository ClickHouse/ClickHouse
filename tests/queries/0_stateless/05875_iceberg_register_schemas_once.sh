#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

TABLE="t_${CLICKHOUSE_DATABASE}_register_schemas_once"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}_no_cache"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT
rm -rf "${TABLE_PATH}"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${TABLE} SETTINGS allow_insert_into_iceberg = 1 VALUES (1)"
FIRST_SNAPSHOT=$(${CLICKHOUSE_CLIENT} --query "SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = '${TABLE}'")
${CLICKHOUSE_CLIENT} --multiquery --query "
    SET allow_insert_into_iceberg = 1;
    ALTER TABLE ${TABLE} ADD COLUMN a Nullable(Int64);
    INSERT INTO ${TABLE} VALUES (2, 20);
    ALTER TABLE ${TABLE} ADD COLUMN b Nullable(String);
    INSERT INTO ${TABLE} VALUES (3, 30, 'x');
    ALTER TABLE ${TABLE} RENAME COLUMN a TO a2;
    INSERT INTO ${TABLE} VALUES (4, 40, 'y');
"

run()
{
    local name=$1
    shift
    echo "-- ${name}"
    ${CLICKHOUSE_CLIENT} --log_comment "${TABLE}_${name}" "$@"
}

run cached_1 --query "SELECT * FROM ${TABLE} ORDER BY id"
run cached_2 --query "SELECT * FROM ${TABLE} ORDER BY id"
run time_travel_1 --iceberg_snapshot_id "${FIRST_SNAPSHOT}" --query "SELECT * FROM ${TABLE} ORDER BY id"
run time_travel_2 --iceberg_snapshot_id "${FIRST_SNAPSHOT}" --query "SELECT * FROM ${TABLE} ORDER BY id"

${CLICKHOUSE_CLIENT} --multiquery --query "
    SET allow_insert_into_iceberg = 1;
    ALTER TABLE ${TABLE} ADD COLUMN c Nullable(Float64);
    INSERT INTO ${TABLE} VALUES (5, 50, 'z', 0.5);
"

run evolved --query "SELECT * FROM ${TABLE} ORDER BY id"

${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache 0 --query "CREATE TABLE ${TABLE}_no_cache ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"
run no_cache_1 --query "SELECT * FROM ${TABLE}_no_cache ORDER BY id"
run no_cache_2 --query "SELECT * FROM ${TABLE}_no_cache ORDER BY id"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
echo "-- profile events"
${CLICKHOUSE_CLIENT} --query "
    SELECT
        substring(log_comment, length('${TABLE}_') + 1) AS name,
        ProfileEvents['IcebergMetadataJSONSchemaRegistrations'] > 0 AS registered,
        ProfileEvents['IcebergMetadataJSONSchemaRegistrationSkips'] > 0 AS skipped
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(log_comment, '${TABLE}_')
    ORDER BY event_time_microseconds
"
