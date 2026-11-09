#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option) and S3

# `OPTIMIZE` must read a data file written before `RENAME COLUMN` by the file's own schema and map it
# to the current one, otherwise the renamed column is compacted into NULLs. The rows are rolled over
# into three data files of one manifest, so every file of that manifest must be rewritten, not just
# the first. Reads apply the position delete whether or not compaction ran, so it must be gone
# afterwards for a silent no-op to fail.
#
# The cloud build routes `OPTIMIZE` through a different code path, so the compaction is not asserted there.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1"

# $1: storage label, $2: table name, $3: engine
run_case()
{
    local storage="$1" table="$2" engine="$3"

    $CH --query "CREATE TABLE ${table} (id Int32, value Nullable(String)) ENGINE = ${engine}"
    # One insert sink: each sink commits its own snapshot and manifest.
    $CH --iceberg_insert_max_rows_in_data_file=2 --min_insert_block_size_rows=2 --max_insert_block_size=2 \
        --max_block_size=2 --max_insert_threads=1 \
        --query "INSERT INTO ${table} SELECT number + 1, char(97 + number) FROM numbers(6)"
    $CH --query "DELETE FROM ${table} WHERE id = 3"
    $CH --query "ALTER TABLE ${table} RENAME COLUMN value TO label"

    echo "--- ${storage}: position deletes, snapshots and data files before OPTIMIZE"
    $CH --query "
        SELECT countIf(content = 'POSITION_DELETE'), uniqExactIf(snapshot_id, content = 'DATA'), countIf(content = 'DATA')
        FROM system.iceberg_files WHERE database = currentDatabase() AND table = '${table}'"

    local optimize_err position_deletes=0
    optimize_err=$($CH --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${table}" 2>&1)
    if [[ "${IS_CLOUD}" != "1" ]]; then
        [[ -n "${optimize_err}" ]] && echo "FAIL: OPTIMIZE failed: ${optimize_err}"
        position_deletes=$($CH --query "
            SELECT count() FROM system.iceberg_files
            WHERE database = currentDatabase() AND table = '${table}' AND content = 'POSITION_DELETE'")
    fi

    echo "--- ${storage}: after OPTIMIZE"
    echo "position deletes: ${position_deletes}"
    $CH --query "SELECT count() FROM ${table}"
    $CH --query "SELECT id, label FROM ${table} ORDER BY id"

    $CH --query "DROP TABLE ${table} SYNC"
}

local_table="t_${CLICKHOUSE_DATABASE}_local"
local_path="${USER_FILES_PATH:?}/${local_table}/"
run_case local "${local_table}" "IcebergLocal('${local_path}', 'Parquet')"
rm -rf "${local_path}"

run_case s3 "t_${CLICKHOUSE_DATABASE}_s3" "IcebergS3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}/', format = 'Parquet')"
