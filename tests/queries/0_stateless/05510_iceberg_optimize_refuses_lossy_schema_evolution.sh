#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option) and S3

# Compaction rewrites the historical data files into the current schema while every snapshot keeps its
# own schema id. So `OPTIMIZE` must be refused before it writes anything when a schema that a snapshot
# still references differs from the current one: a dropped column (time travel would lose it), an
# added column (the old schema cannot resolve it), or a required column made optional (the rewrite
# aborts on the still-required data). With no position delete there is nothing to rewrite, and
# `OPTIMIZE` stays a no-op. Format version 3 is refused because the rewrite drops row lineage.
#
# The cloud build routes `OPTIMIZE` through a different code path, so only the open-source build must
# report these refusals.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

IS_CLOUD=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")

CH="${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1"

# $1: table, $2: error code, $3: the refusal's own wording, since unrelated refusals share the code.
optimize_refused()
{
    local err
    err=$($CH --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE $1" 2>&1)
    if [[ "${IS_CLOUD}" = "1" || ( "${err}" == *"$2"* && "${err}" == *"$3"* ) ]]; then
        echo "OPTIMIZE refused"
    else
        echo "FAIL: OPTIMIZE was not refused with $2 '$3': ${err}"
    fi
}

position_deletes()
{
    $CH --query "
        SELECT count() FROM system.iceberg_files
        WHERE database = currentDatabase() AND table = '$1' AND content = 'POSITION_DELETE'"
}

# The `next-row-id` of the metadata version that a read of $1 resolves. With parallel replicas every
# replica logs that metadata under the same query id.
next_row_id()
{
    local query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}${RANDOM}"
    $CH --iceberg_metadata_log_level=metadata --query_id "${query_id}" --query "SELECT count() FROM $1" > /dev/null
    $CH --query "
        SYSTEM FLUSH LOGS iceberg_metadata_log;
        SELECT groupArray(JSONExtractInt(content, 'next-row-id'))
        FROM (SELECT DISTINCT content FROM system.iceberg_metadata_log
              WHERE query_id = '${query_id}' AND content_type = 'Metadata')"
}

# Each case gets $1: storage label, $2: table name, $3: engine.
dropped_column()
{
    $CH --query "CREATE TABLE $2 (id Int32, value Nullable(String), extra Nullable(String)) ENGINE = $3"
    $CH --query "INSERT INTO $2 VALUES (1, 'a', 'x1'), (2, 'b', 'x2'), (3, 'c', 'x3')"
    local pre_drop_snapshot
    pre_drop_snapshot=$($CH --query "
        SELECT snapshot_id FROM system.iceberg_history
        WHERE database = currentDatabase() AND table = '$2' ORDER BY made_current_at DESC LIMIT 1")
    $CH --query "DELETE FROM $2 WHERE id = 2"
    $CH --query "ALTER TABLE $2 DROP COLUMN extra"

    echo "--- $1: dropped column"
    optimize_refused "$2" NOT_IMPLEMENTED "was dropped from the current schema"
    position_deletes "$2"
    $CH --query "SELECT id, value, extra FROM $2 ORDER BY id SETTINGS iceberg_snapshot_id = ${pre_drop_snapshot}"
}

dropped_column_nothing_to_compact()
{
    $CH --query "CREATE TABLE $2 (id Int32, value Nullable(String), extra Nullable(String)) ENGINE = $3"
    $CH --query "INSERT INTO $2 VALUES (1, 'a', 'x1'), (2, 'b', 'x2'), (3, 'c', 'x3')"
    $CH --query "ALTER TABLE $2 DROP COLUMN extra"

    echo "--- $1: dropped column, nothing to compact"
    local err
    err=$($CH --allow_experimental_iceberg_compaction=1 --query "OPTIMIZE TABLE $2" 2>&1)
    if [[ "${IS_CLOUD}" = "1" || -z "${err}" ]]; then
        echo "OPTIMIZE accepted"
    else
        echo "FAIL: OPTIMIZE failed: ${err}"
    fi
    $CH --query "SELECT id, value FROM $2 ORDER BY id"
}

added_column()
{
    $CH --query "CREATE TABLE $2 (id Int32, value Nullable(String)) ENGINE = $3"
    $CH --query "INSERT INTO $2 VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    $CH --query "DELETE FROM $2 WHERE id = 2"
    $CH --query "ALTER TABLE $2 ADD COLUMN extra Nullable(String)"

    echo "--- $1: added column"
    optimize_refused "$2" NOT_IMPLEMENTED "added field id"
    $CH --query "SELECT id, value, extra FROM $2 ORDER BY id"
}

required_column_made_optional()
{
    $CH --query "CREATE TABLE $2 (id Int32, value String) ENGINE = $3"
    $CH --query "INSERT INTO $2 VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    $CH --query "DELETE FROM $2 WHERE id = 2"
    $CH --query "ALTER TABLE $2 MODIFY COLUMN value Nullable(String)"

    echo "--- $1: required column made optional"
    optimize_refused "$2" NOT_IMPLEMENTED "changed type"
    $CH --query "SELECT id, value FROM $2 ORDER BY id"
}

# `DELETE` is refused on format version 3, and the version check does not depend on there being work to do.
format_version_3()
{
    $CH --query "CREATE TABLE $2 (id Int32, value Nullable(String)) ENGINE = $3 SETTINGS iceberg_format_version = 3"
    $CH --query "INSERT INTO $2 VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    $CH --query "INSERT INTO $2 VALUES (4, 'd'), (5, 'e')"

    echo "--- $1: format version 3"
    local before="[5]" after="[5]"
    [[ "${IS_CLOUD}" != "1" ]] && before=$(next_row_id "$2")
    optimize_refused "$2" BAD_ARGUMENTS "Compaction is supported only for format_version 2"
    [[ "${IS_CLOUD}" != "1" ]] && after=$(next_row_id "$2")
    echo "next-row-id before and after OPTIMIZE: ${before} ${after}"
    $CH --query "SELECT id, value FROM $2 ORDER BY id"
}

# $1: case, $2: storage label
run_case()
{
    local table="t_${CLICKHOUSE_DATABASE}_$1_$2"
    if [[ "$2" = "local" ]]; then
        "$1" "$2" "${table}" "IcebergLocal('${USER_FILES_PATH:?}/${table}/', 'Parquet')"
        $CH --query "DROP TABLE ${table} SYNC"
        rm -rf "${USER_FILES_PATH:?}/${table}/"
    else
        "$1" "$2" "${table}" "IcebergS3(s3_conn, filename = '${CLICKHOUSE_TEST_UNIQUE_NAME}/$1/', format = 'Parquet')"
        $CH --query "DROP TABLE ${table} SYNC"
    fi
}

for storage in local s3; do
    run_case dropped_column "${storage}"
    run_case dropped_column_nothing_to_compact "${storage}"
done
run_case added_column local
run_case required_column_made_optional local
run_case format_version_3 local
