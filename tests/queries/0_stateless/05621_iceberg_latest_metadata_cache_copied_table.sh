#!/usr/bin/env bash
# Tags: no-fasttest
# A copy of an Iceberg table directory carries the same `table-uuid`; the latest metadata file cached for the copy
# under `iceberg_metadata_staleness_ms` must not be served to the original.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${DIR}"
mkdir -p "${DIR}"
trap 'rm -rf "${DIR}"' EXIT

${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 -m -q "
    CREATE TABLE original (c0 Int32) ENGINE = IcebergLocal('${DIR}/original/');
    INSERT INTO original VALUES (1);
" -- --user_files_path="${DIR}"

cp -r "${DIR}/original" "${DIR}/copy"

# Both tables in one process, so they share one cache.
${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=1 --iceberg_metadata_staleness_ms=600000 -m -q "
    CREATE TABLE IF NOT EXISTS copy (c0 Int32) ENGINE = IcebergLocal('${DIR}/copy/');
    INSERT INTO copy VALUES (2), (3);
    SELECT 'copy', count() FROM copy;
    CREATE TABLE IF NOT EXISTS original (c0 Int32) ENGINE = IcebergLocal('${DIR}/original/');
    SELECT 'original', c0 FROM original ORDER BY c0;
    SELECT 'original', c0 FROM original ORDER BY c0;
" -- --user_files_path="${DIR}"
