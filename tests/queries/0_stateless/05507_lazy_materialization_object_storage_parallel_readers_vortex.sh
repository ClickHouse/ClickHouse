#!/usr/bin/env bash
# Tags: no-fasttest, no-msan, no-parallel-replicas
# - no-fasttest, no-msan: the Vortex format is not included in the fast test and MSan builds; requires MinIO and `IcebergLocal`
# - no-parallel-replicas: asserts on the query plan, see 05507_lazy_materialization_object_storage_parallel_readers

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Vortex reads whole top-level fields and converts them to the requested types, so `t.c1`, a Tuple of some elements of `t`
# and `t` read as `String` all read the whole struct `t`, and the lazy read of Vortex files keeps one reader for any column.
# `t.c1` is also a tuple element of an Iceberg table in the `Parquet` format whose data files are Vortex
# (only the data files tell the format then).

S3_URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_lazy_parallel_vortex"
TABLE="t_${CLICKHOUSE_DATABASE}_lazy_parallel_vortex"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
TUPLE_TYPE="Tuple(c1 String, c2 String, c3 String, c4 String, c5 String, c6 String, c7 String, c8 String, c9 String, c10 String)"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE}_written (k Int64, ts Int64, s String, t ${TUPLE_TYPE}) ENGINE = IcebergLocal('${TABLE_PATH}', 'Vortex')
"

# Eight data files of each kind.
for i in {0..7}; do
    data="
        SELECT number AS k, cityHash64(number) % 1000003 AS ts, concat('val_', toString(number)) AS s,
            (toString(number), concat('a', s), concat('b', s), concat('c', s), concat('d', s), concat('e', s), concat('f', s),
                concat('g', s), concat('h', s), concat('i', s)) AS t
        FROM numbers($((i * 2000)), 2000)"
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION s3('${S3_URL}/data_${i}.vortex', 'test', 'testtest', 'Vortex', 'k Int64, ts Int64, s String, t ${TUPLE_TYPE}')
        ${data} SETTINGS s3_truncate_on_insert = 1
    "
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --max_insert_threads=1 --query "INSERT INTO ${TABLE}_written ${data}"
done

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"

# The main pass reads `ts` and `k`; the lazy pass reads 1 leaf column for `s` and the struct `t` for the others.
SUBCOLUMN="SELECT k, \`t.c1\` FROM s3('${S3_URL}/data_*.vortex', 'test', 'testtest', 'Vortex', 'k Int64, ts Int64, \`t.c1\` String') ORDER BY ts, k LIMIT 100"
NARROW="SELECT k, s FROM s3('${S3_URL}/data_*.vortex', 'test', 'testtest', 'Vortex', 'k Int64, ts Int64, s String') ORDER BY ts, k LIMIT 100"
TUPLE="SELECT k, t FROM s3('${S3_URL}/data_*.vortex', 'test', 'testtest', 'Vortex', 'k Int64, ts Int64, t ${TUPLE_TYPE}') ORDER BY ts, k LIMIT 100"
NARROW_TUPLE="SELECT k, t FROM s3('${S3_URL}/data_*.vortex', 'test', 'testtest', 'Vortex', 'k Int64, ts Int64, t Tuple(c1 String)') ORDER BY ts, k LIMIT 100"
STRING="SELECT k, t FROM s3('${S3_URL}/data_*.vortex', 'test', 'testtest', 'Vortex', 'k Int64, ts Int64, t String') ORDER BY ts, k LIMIT 100"
ICEBERG_SUBCOLUMN="SELECT k, t.c1 FROM ${TABLE} ORDER BY ts, k LIMIT 100"

# See 05507_lazy_materialization_object_storage_parallel_readers for the settings.
LAZY_SETTINGS=(--query_plan_optimize_lazy_materialization=1 --query_plan_max_limit_for_lazy_materialization=0 --max_threads_min_free_memory_per_thread=0
    --use_concurrency_control_for_max_threads=0 --s3_validate_etag_on_read=1 --max_threads=16)

echo '-- the lazy read step is in the plan'
for query in "$SUBCOLUMN" "$ICEBERG_SUBCOLUMN"; do
    ${CLICKHOUSE_CLIENT} "${LAZY_SETTINGS[@]}" --query_plan_optimize_lazy_materialization_for_object_storage=1 \
        --query "EXPLAIN ${query}" | grep -c 'LazilyReadFromObjectStorage'
done

echo '-- the same result with the optimization enabled and disabled'
for query in "$SUBCOLUMN" "$ICEBERG_SUBCOLUMN"; do
    results=()
    for enabled in 1 0; do
        results+=("$(${CLICKHOUSE_CLIENT} "${LAZY_SETTINGS[@]}" --max_block_size=7 \
            --query_plan_optimize_lazy_materialization_for_object_storage="$enabled" --query "$query" | md5sum)")
    done
    [ "${results[0]}" = "${results[1]}" ] && echo 'same' || echo 'different'
done

QUERY_ID_PREFIX="${CLICKHOUSE_DATABASE}_$(random_str 10)_readers"
run_counted()
{
    local kind=$1 query=$2
    ${CLICKHOUSE_CLIENT} "${LAZY_SETTINGS[@]}" --query_plan_optimize_lazy_materialization_for_object_storage=1 \
        --log_processors_profiles=1 --query_id="${QUERY_ID_PREFIX}_${kind}" --query "$query FORMAT Null"
}
run_counted subcolumn "$SUBCOLUMN"
run_counted narrow "$NARROW"
run_counted tuple "$TUPLE"
run_counted narrow_tuple "$NARROW_TUPLE"
run_counted string "$STRING"
run_counted iceberg_subcolumn "$ICEBERG_SUBCOLUMN"

echo '-- the number of readers'
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS processors_profile_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(query_id, '${QUERY_ID_PREFIX}_', '') AS kind, countIf(name = 'LazyReadFromObjectStorageSource') - 1 AS readers
    FROM system.processors_profile_log
    WHERE event_date >= yesterday() AND startsWith(query_id, '${QUERY_ID_PREFIX}_')
    GROUP BY query_id ORDER BY query_id
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}_written"
