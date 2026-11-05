#!/usr/bin/env bash
# Tags: no-fasttest, no-msan, no-parallel-replicas
# - no-fasttest, no-msan: the Vortex format is not included in the fast test and MSan builds; requires MinIO
# - no-parallel-replicas: asserts on the query plan, see 05507_lazy_materialization_object_storage_parallel_readers

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Vortex reads the column `t.c1` of an explicit schema as the whole struct `t`, so the lazy read of such a column keeps
# one reader, while a plain column or the whole Tuple is read by several readers, as in
# 05507_lazy_materialization_object_storage_parallel_readers.

S3_URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_lazy_parallel_vortex"
TUPLE_TYPE="Tuple(c1 String, c2 String, c3 String, c4 String, c5 String, c6 String, c7 String, c8 String, c9 String, c10 String)"
for i in {0..7}; do
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION s3('${S3_URL}/data_${i}.vortex', 'test', 'testtest', 'Vortex', 'k Int64, ts Int64, s String, t ${TUPLE_TYPE}')
        SELECT number AS k, cityHash64(number) % 1000003 AS ts, concat('val_', toString(number)) AS s,
            (toString(number), concat('a', s), concat('b', s), concat('c', s), concat('d', s), concat('e', s), concat('f', s),
                concat('g', s), concat('h', s), concat('i', s)) AS t
        FROM numbers($((i * 2000)), 2000)
        SETTINGS s3_truncate_on_insert = 1
    "
done

# The main pass reads `ts` and `k`; the lazy pass reads the struct `t` for `t.c1`, 1 leaf column for `s`, 10 for `t`.
SUBCOLUMN="SELECT k, \`t.c1\` FROM s3('${S3_URL}/data_*.vortex', 'test', 'testtest', 'Vortex', 'k Int64, ts Int64, \`t.c1\` String') ORDER BY ts, k LIMIT 100"
NARROW="SELECT k, s FROM s3('${S3_URL}/data_*.vortex', 'test', 'testtest', 'Vortex', 'k Int64, ts Int64, s String') ORDER BY ts, k LIMIT 100"
TUPLE="SELECT k, t FROM s3('${S3_URL}/data_*.vortex', 'test', 'testtest', 'Vortex', 'k Int64, ts Int64, t ${TUPLE_TYPE}') ORDER BY ts, k LIMIT 100"

# See 05507_lazy_materialization_object_storage_parallel_readers for the settings.
LAZY_SETTINGS=(--query_plan_optimize_lazy_materialization=1 --query_plan_max_limit_for_lazy_materialization=0 --max_threads_min_free_memory_per_thread=0
    --s3_validate_etag_on_read=1 --max_threads=16)

echo '-- the lazy read step is in the plan'
${CLICKHOUSE_CLIENT} "${LAZY_SETTINGS[@]}" --query_plan_optimize_lazy_materialization_for_object_storage=1 \
    --query "EXPLAIN ${SUBCOLUMN}" | grep -c 'LazilyReadFromObjectStorage'

echo '-- the same result with the optimization enabled and disabled'
results=()
for enabled in 1 0; do
    results+=("$(${CLICKHOUSE_CLIENT} "${LAZY_SETTINGS[@]}" --max_block_size=7 \
        --query_plan_optimize_lazy_materialization_for_object_storage="$enabled" --query "$SUBCOLUMN" | md5sum)")
done
[ "${results[0]}" = "${results[1]}" ] && echo 'same' || echo 'different'

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

echo '-- the number of readers'
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS processors_profile_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(query_id, '${QUERY_ID_PREFIX}_', '') AS kind, countIf(name = 'LazyReadFromObjectStorageSource') - 1 AS readers
    FROM system.processors_profile_log
    WHERE event_date >= yesterday() AND startsWith(query_id, '${QUERY_ID_PREFIX}_')
    GROUP BY query_id ORDER BY query_id
"
