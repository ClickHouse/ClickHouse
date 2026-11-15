#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_DIR="${USER_FILES_PATH}/t_${CLICKHOUSE_DATABASE}"
PFX="t_${CLICKHOUSE_DATABASE}"
CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0"

cleanup() {
    ${CH} -q "DROP TABLE IF EXISTS ${PFX}_ASC; DROP TABLE IF EXISTS ${PFX}_DESC;" > /dev/null 2>&1
    rm -rf "${BASE_DIR}"
}
cleanup
trap cleanup EXIT

for dir in ASC DESC; do
    echo "-- ORDER BY x ${dir}"
    ${CH} -q "CREATE TABLE ${PFX}_${dir} (x Nullable(Int64)) ENGINE = IcebergLocal('${BASE_DIR}/${dir}/') ORDER BY x ${dir}"
    # The declared order, read before any INSERT.
    ${CH} -q "SELECT arrayMap(f -> (JSONExtractString(f, 'direction'), JSONExtractString(f, 'null-order')),
                  JSONExtractArrayRaw(json, 'sort-orders', 1, 'fields'))
              FROM file('${BASE_DIR}/${dir}/metadata/v1.metadata.json', JSONAsString, 'json String')"
    ${CH} -q "INSERT INTO ${PFX}_${dir} SELECT if(number % 3 = 0, NULL, number) FROM numbers(9) SETTINGS max_threads = 1, max_insert_threads = 1"
    # The order INSERT wrote, one row per line (groupArray would drop the NULLs).
    ${CH} -q "SELECT x FROM file('${BASE_DIR}/${dir}/data/*.parquet', Parquet) SETTINGS max_threads = 1"
done
