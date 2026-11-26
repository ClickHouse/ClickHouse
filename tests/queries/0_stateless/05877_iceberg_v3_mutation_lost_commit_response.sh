#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# - no-fasttest: needs Iceberg writes.
# - no-parallel: the fail point is global, and the mutation of another test could hit it.

# A mutation of a format-version 3 Iceberg table whose metadata file was written, but whose response was lost, is
# published. The mutation must recognize its own snapshot and succeed, instead of removing the deletion vectors and
# manifests that the published snapshot references.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap '${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT iceberg_mutation_lose_metadata_commit_response" ||:; rm -rf "${TABLE_PATH}"' EXIT

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE ${TABLE} (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') SETTINGS iceberg_format_version = 3;
    INSERT INTO ${TABLE} SELECT number FROM numbers(10);"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_mutation_lose_metadata_commit_response"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "DELETE FROM ${TABLE} WHERE id < 3"

echo "metadata files: $(cd "${TABLE_PATH}metadata" && ls -- *.metadata.json | sort -V | paste -sd ' ')"
echo "puffin files: $(find "${TABLE_PATH}" -name '*.puffin' | wc -l)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id) FROM ${TABLE}"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(id) FROM icebergLocal('${TABLE_PATH}', 'Parquet')"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE} SYNC"
