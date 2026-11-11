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

# Spellings of one directory open the same files: an insert through one is seen through the others,
# and each keeps its own cached path, so a fresh open does not read through a removed symlink.
TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
LINK="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_link"
trap 'rm -rf "${DIR}" "${TABLE_PATH}" "${LINK}"' EXIT
CLIENT="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=1 --iceberg_metadata_staleness_ms=600000"
${CLIENT} -q "CREATE TABLE plain (c0 Int32) ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLIENT} -q "INSERT INTO plain VALUES (1)"
ln -s "${CLICKHOUSE_TEST_UNIQUE_NAME}" "${LINK}"
${CLIENT} -q "CREATE TABLE dotted ENGINE = IcebergLocal('${TABLE_PATH}./')"
${CLIENT} -q "CREATE TABLE linked ENGINE = IcebergLocal('${LINK}/')"
${CLIENT} -q "SELECT 'plain', count() FROM plain"
${CLIENT} -q "SELECT 'dotted', count() FROM dotted"
${CLIENT} -q "INSERT INTO linked VALUES (2)"
${CLIENT} -q "SELECT 'linked', count() FROM linked"
${CLIENT} -q "SELECT 'plain', count() FROM plain"
${CLIENT} -q "SELECT 'dotted', count() FROM dotted"
rm "${LINK}"
${CLIENT} -q "CREATE TABLE reopened ENGINE = IcebergLocal('${TABLE_PATH}')"
${CLIENT} -q "SELECT 'reopened', count() FROM reopened"

# Another process's insert is not seen within the staleness window: the reads above were served from the cache.
${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 -m -q "
    CREATE TABLE writer ENGINE = IcebergLocal('${TABLE_PATH}');
    INSERT INTO writer VALUES (3);
"
${CLIENT} -q "SELECT 'plain', count() FROM plain"
