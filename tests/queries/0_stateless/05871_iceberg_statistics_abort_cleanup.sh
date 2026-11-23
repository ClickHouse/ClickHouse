#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires `IcebergLocal` and DataSketches.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR:?}"
mkdir -p "${WORK_DIR}"
trap 'rm -rf "${WORK_DIR}"' EXIT

local_query()
{
    ${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --iceberg_write_table_statistics_ndv=1 \
        --max_insert_threads=1 --query "$1" -- --user_files_path="${WORK_DIR}"
}

local_query "
    CREATE TABLE t (id Int64) ENGINE = IcebergLocal('${WORK_DIR}/table/');
    INSERT INTO t VALUES (1);"

# A successful commit must write statistics, so the cleanup check cannot pass with statistics disabled.
echo "committed statistics: $(find "${WORK_DIR}/table" -name '*.stats' | wc -l)"
find "${WORK_DIR}/table" -type f | sort > "${WORK_DIR}/before"

# `iceberg_writes_cleanup` throws after statistics are written and before the metadata commit.
if local_query "
    CREATE TABLE t ENGINE = IcebergLocal('${WORK_DIR}/table/');
    SYSTEM ENABLE FAILPOINT iceberg_writes_cleanup;
    INSERT INTO t VALUES (2);" 2> "${WORK_DIR}/error.log"; then
    echo "Expected the insert to fail"
    exit 1
fi
grep -qF 'Failpoint for cleanup enabled' "${WORK_DIR}/error.log"

find "${WORK_DIR}/table" -type f | sort > "${WORK_DIR}/after"
diff -u "${WORK_DIR}/before" "${WORK_DIR}/after"
echo "remaining statistics: $(find "${WORK_DIR}/table" -name '*.stats' | wc -l)"
local_query "CREATE TABLE t ENGINE = IcebergLocal('${WORK_DIR}/table/'); SELECT id FROM t;"
