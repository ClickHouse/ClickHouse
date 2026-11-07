#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option) and S3

# The metadata `OPTIMIZE` publishes must keep the table's partition spec, so that an INSERT after the
# compaction still writes partitioned data files. Filtered reads cannot show a lost spec: `part` is a
# stored column, so `WHERE part = ...` is right even over unpartitioned files. The persisted spec and
# the partition tuples of the new files are asserted instead.
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

    $CH --query "CREATE TABLE ${table} (part Int32, id Int32, value Nullable(String)) ENGINE = ${engine} PARTITION BY part"
    $CH --query "INSERT INTO ${table} VALUES (0, 1, 'a'), (0, 2, 'b'), (1, 3, 'c'), (1, 4, 'd')"
    $CH --query "DELETE FROM ${table} WHERE id = 2"

    local optimize_err position_deletes=0 spec="1	['part']"
    optimize_err=$($CH --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE ${table}" 2>&1)
    if [[ "${IS_CLOUD}" != "1" ]]; then
        [[ -n "${optimize_err}" ]] && echo "FAIL: OPTIMIZE failed: ${optimize_err}"
        position_deletes=$($CH --query "
            SELECT count() FROM system.iceberg_files
            WHERE database = currentDatabase() AND table = '${table}' AND content = 'POSITION_DELETE'")

        # One read resolves one metadata version, so exactly one logged version is expected.
        local query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${storage}_${RANDOM}"
        $CH --iceberg_metadata_log_level=metadata --query_id "${query_id}" --query "SELECT count() FROM ${table}" > /dev/null
        spec=$($CH --query "
            SYSTEM FLUSH LOGS iceberg_metadata_log;
            SELECT count(), any(arrayMap(f -> JSONExtractString(f, 'name'), JSONExtractArrayRaw(
                arrayFirst(s -> JSONExtractInt(s, 'spec-id') = JSONExtractInt(content, 'default-spec-id'),
                    JSONExtractArrayRaw(content, 'partition-specs')), 'fields')))
            FROM (SELECT DISTINCT content FROM system.iceberg_metadata_log
                  WHERE query_id = '${query_id}' AND content_type = 'Metadata')")
    fi

    echo "--- ${storage}: after OPTIMIZE"
    echo "position deletes: ${position_deletes}"
    $CH --query "SELECT count() FROM ${table}"
    echo "default partition spec: ${spec}"

    $CH --query "INSERT INTO ${table} VALUES (0, 5, 'e'), (1, 6, 'f')"

    echo "--- ${storage}: after a later INSERT"
    $CH --query "
        SELECT countIf(partition = '{}') FROM system.iceberg_files
        WHERE database = currentDatabase() AND table = '${table}' AND content = 'DATA'"
    $CH --query "SELECT id, value FROM ${table} WHERE part = 0 ORDER BY id"
    $CH --query "SELECT id, value FROM ${table} WHERE part = 1 ORDER BY id"
    $CH --query "SELECT count() FROM ${table}"

    $CH --query "DROP TABLE ${table} SYNC"
}

local_table="t_${CLICKHOUSE_DATABASE}_local"
local_path="${USER_FILES_PATH:?}/${local_table}/"
run_case local "${local_table}" "IcebergLocal('${local_path}', 'Parquet')"
rm -rf "${local_path}"

run_case s3 "t_${CLICKHOUSE_DATABASE}_s3" "IcebergS3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}/', format = 'Parquet')"
