#!/usr/bin/env bash
# Tags: no-fasttest
# Two materialized views write into the same table, which has a materialized view with an Iceberg target.
# The Iceberg target is reached by two paths of one INSERT, but it is committed once, in one snapshot.
# The second INSERT writes the same rows again, in one more snapshot.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="${CLICKHOUSE_DATABASE}_${RANDOM}"
SRC="src_${PREFIX}"
HUB="hub_${PREFIX}"
OUT="out_${PREFIX}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "
        DROP TABLE IF EXISTS mv_out_${PREFIX}; DROP TABLE IF EXISTS mv_a_${PREFIX}; DROP TABLE IF EXISTS mv_b_${PREFIX};
        DROP TABLE IF EXISTS ${SRC}; DROP TABLE IF EXISTS ${HUB}; DROP TABLE IF EXISTS ${OUT}" >/dev/null 2>&1
    rm -rf "${USER_FILES_PATH:?}/${OUT}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${SRC} (k Int64) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE ${HUB} (k Int64) ENGINE = MergeTree ORDER BY k;
    CREATE TABLE ${OUT} (k Int64) ENGINE = IcebergLocal('${USER_FILES_PATH}/${OUT}/', 'Parquet');
    CREATE MATERIALIZED VIEW mv_a_${PREFIX} TO ${HUB} AS SELECT k FROM ${SRC};
    CREATE MATERIALIZED VIEW mv_b_${PREFIX} TO ${HUB} AS SELECT k + 1000000 AS k FROM ${SRC};
    CREATE MATERIALIZED VIEW mv_out_${PREFIX} TO ${OUT} AS SELECT k FROM ${HUB}"

# Few blocks: each of them runs the views four times.
for max_insert_threads in 1 4
do
    echo "=== max_insert_threads = ${max_insert_threads} ==="
    ${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE ${SRC}; TRUNCATE TABLE ${HUB}"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --max_insert_threads="${max_insert_threads}" --max_threads=4 \
        --max_block_size=100 --parallel_view_processing=1 \
        --max_insert_threads_min_free_memory_per_thread=0 --max_threads_min_free_memory_per_thread=0 \
        --max_insert_block_size=100 --min_insert_block_size_rows=100 --min_insert_block_size_bytes=1 \
        --query "INSERT INTO ${SRC} SELECT number FROM numbers_mt(1000)"

    ${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(k) FROM ${HUB}"
    ${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(k) FROM ${OUT}"
    echo "snapshots: $(find "${USER_FILES_PATH}/${OUT}/metadata" -maxdepth 1 -name 'snap-*.avro' -type f | wc -l | tr -d ' ')"
    # The counts in the metadata of the last snapshot agree with the scan.
    ${CLICKHOUSE_CLIENT} --query "
        SELECT summary['added-records'], summary['total-records']
        FROM system.iceberg_history
        WHERE database = currentDatabase() AND table = '${OUT}'
        ORDER BY made_current_at DESC
        LIMIT 1"
    # No manifest is listed twice in a manifest list.
    echo "unique manifests: $(${CLICKHOUSE_CLIENT} --query "
        SELECT min(manifests = unique_manifests)
        FROM
        (
            SELECT count() AS manifests, uniqExact(manifest_path) AS unique_manifests
            FROM file('${USER_FILES_PATH}/${OUT}/metadata/snap-*.avro', 'Avro', 'manifest_path String')
            GROUP BY _file
        )")"
done
