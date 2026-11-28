#!/usr/bin/env bash
# Tags: no-fasttest
# The Iceberg targets of materialized views are written by the parallel sinks of an INSERT and committed in one snapshot
# when the whole INSERT has finished. A failed view fails the INSERT, and then nothing is committed, the source table included.
# With `materialized_views_ignore_errors`, everything is committed, without what the sink which got the error has written.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="${CLICKHOUSE_DATABASE}_${RANDOM}"
SRC="src_${PREFIX}"
TGT="tgt_${PREFIX}"
ICE_SRC="ice_src_${PREFIX}"
TGT_OK="tgt_ok_${PREFIX}"
TGT_BAD="tgt_bad_${PREFIX}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS mv_${PREFIX}; DROP TABLE IF EXISTS mv_ok_${PREFIX}; DROP TABLE IF EXISTS mv_bad_${PREFIX};
        DROP TABLE IF EXISTS ${SRC}; DROP TABLE IF EXISTS ${TGT}; DROP TABLE IF EXISTS ${ICE_SRC};
        DROP TABLE IF EXISTS ${TGT_OK}; DROP TABLE IF EXISTS ${TGT_BAD}" >/dev/null 2>&1
    rm -rf "${USER_FILES_PATH:?}/${TGT}" "${USER_FILES_PATH:?}/${ICE_SRC}" "${USER_FILES_PATH:?}/${TGT_OK}" "${USER_FILES_PATH:?}/${TGT_BAD}"
}
trap cleanup EXIT

create_iceberg_table()
{
    ${CLICKHOUSE_CLIENT} --query "
        CREATE TABLE $1 (k Int64)
        ENGINE = IcebergLocal('${USER_FILES_PATH}/$1/', 'Parquet')
        PARTITION BY icebergBucket(2, k)"
}

# Rows and snapshots of an Iceberg table.
describe()
{
    echo "$2: $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM $1") rows," \
        "$(find "${USER_FILES_PATH}/$1/metadata" -maxdepth 1 -name 'snap-*.avro' -type f | wc -l | tr -d ' ') snapshots"
}

# Small blocks spread the insert over all the parallel sinks. The data file limits are above the insert size,
# so each sink writes at most one file per partition: more than two files in a snapshot mean several sinks.
SETTINGS="--allow_insert_into_iceberg=1 --max_insert_threads=4 --max_threads=4 --max_block_size=100 --parallel_view_processing=1
    --max_insert_threads_min_free_memory_per_thread=0 --max_threads_min_free_memory_per_thread=0
    --max_insert_block_size=100 --min_insert_block_size_rows=100 --min_insert_block_size_bytes=1
    --iceberg_insert_max_rows_in_data_file=1000000 --iceberg_insert_max_bytes_in_data_file=1000000000"

echo "=== MergeTree source, Iceberg target ==="
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${SRC} (k Int64) ENGINE = MergeTree ORDER BY k"
create_iceberg_table "${TGT}"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW mv_${PREFIX} TO ${TGT} AS SELECT k FROM ${SRC}"

${CLICKHOUSE_CLIENT} ${SETTINGS} --query "INSERT INTO ${SRC} SELECT number FROM numbers_mt(10000)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${SRC}"
describe "${TGT}" "target"
echo "several sinks: $(${CLICKHOUSE_CLIENT} --query "
    SELECT toUInt64(summary['added-data-files']) > 2
    FROM system.iceberg_history
    WHERE database = currentDatabase() AND table = '${TGT}'")"

echo "=== Iceberg source, two Iceberg targets, one view fails ==="
create_iceberg_table "${ICE_SRC}"
create_iceberg_table "${TGT_OK}"
create_iceberg_table "${TGT_BAD}"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW mv_ok_${PREFIX} TO ${TGT_OK} AS SELECT k FROM ${ICE_SRC}"
${CLICKHOUSE_CLIENT} --query "
    CREATE MATERIALIZED VIEW mv_bad_${PREFIX} TO ${TGT_BAD} AS SELECT k FROM ${ICE_SRC} WHERE NOT throwIf(k = 7777)"

${CLICKHOUSE_CLIENT} ${SETTINGS} --query "INSERT INTO ${ICE_SRC} SELECT number FROM numbers_mt(10000)" 2>&1 \
    | grep -o -m1 "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO"
describe "${ICE_SRC}" "source"
describe "${TGT_OK}" "target of the good view"
describe "${TGT_BAD}" "target of the failed view"

echo "=== the same with materialized_views_ignore_errors ==="
${CLICKHOUSE_CLIENT} ${SETTINGS} --materialized_views_ignore_errors=1 --send_logs_level=fatal \
    --query "INSERT INTO ${ICE_SRC} SELECT number FROM numbers_mt(10000)"
describe "${ICE_SRC}" "source"
describe "${TGT_OK}" "target of the good view"
# The rows of the other sinks: how many, depends on how the rows were spread over the sinks.
echo "target of the failed view has a part of the rows: $(${CLICKHOUSE_CLIENT} --query "SELECT count() < 10000 FROM ${TGT_BAD}")"
