#!/usr/bin/env bash
# Tags: no-fasttest, atomic-database
# Incremental refreshable MV with `max_insert_threads > 1` into an Iceberg target that has a dependent materialized view:
# the parallel sinks cannot hand their files to one commit, so the round is written by a single sink, in one snapshot.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SRC="src_${CLICKHOUSE_DATABASE}_${RANDOM}"
TGT="tgt_${CLICKHOUSE_DATABASE}_${RANDOM}"
MV="mv_${CLICKHOUSE_DATABASE}_${RANDOM}"
DEP="dep_${CLICKHOUSE_DATABASE}_${RANDOM}"
DEP_MV="dep_mv_${CLICKHOUSE_DATABASE}_${RANDOM}"
TGT_PATH="${USER_FILES_PATH}/${TGT}/"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS ${MV}; DROP TABLE IF EXISTS ${DEP_MV}; DROP TABLE IF EXISTS ${DEP};
        DROP TABLE IF EXISTS ${TGT}; DROP TABLE IF EXISTS ${SRC}" >/dev/null 2>&1
    rm -rf "${TGT_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${SRC} (k Int64)
    ENGINE = MergeTree ORDER BY k
    SETTINGS
        enable_block_number_column = 1,
        enable_block_offset_column = 1,
        add_minmax_index_for_block_number_column = 1,
        add_minmax_index_for_block_offset_column = 1,
        part_minmax_index_columns = 'with_block_number_offset'
"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TGT} (k Int64)
    ENGINE = IcebergLocal('${TGT_PATH}', 'Parquet')
    PARTITION BY icebergBucket(2, k)
"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DEP} (k Int64) ENGINE = MergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW ${DEP_MV} TO ${DEP} AS SELECT k FROM ${TGT}"

# Small blocks would spread the round over all the parallel sinks.
${CLICKHOUSE_CLIENT} --query "
    CREATE MATERIALIZED VIEW ${MV}
        REFRESH EVERY 10 YEAR APPEND INCREMENTAL
        TO ${TGT} EMPTY
        AS SELECT k FROM ${SRC}
        SETTINGS max_insert_threads = 4, max_threads = 4, max_block_size = 100,
            max_insert_threads_min_free_memory_per_thread = 0, max_threads_min_free_memory_per_thread = 0,
            min_insert_block_size_rows = 100, min_insert_block_size_bytes = 1, parallel_view_processing = 1
"

function refresh_round()
{
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO ${SRC} SELECT number FROM numbers($1, $2)"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM REFRESH VIEW ${MV}"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT VIEW ${MV}"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(k) FROM ${TGT}"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${DEP}"
}

refresh_round 0 10000
refresh_round 10000 5000

echo "=== snapshots: one per round ==="
find "${TGT_PATH}metadata" -maxdepth 1 -name 'snap-*.avro' -type f | wc -l | tr -d ' '
