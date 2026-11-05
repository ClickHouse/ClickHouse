#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-parallel-replicas: asserts on the query plan, which with parallel replicas reads the table with `ReadFromCluster`

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Lazy materialization reads the deferred columns of the data files with surviving rows by up to
# `max_threads` readers, each over a contiguous group of files, and returns their chunks in group order.
# The readers together read no more leaf columns at once than the main pass, so a query that reads
# many columns lazily (or a wide Tuple, whose elements are separate columns in the file) uses fewer readers.

TABLE="t_${CLICKHOUSE_DATABASE}_lazy_parallel"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap 'rm -rf "${TABLE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (
        k Int64, ts Int64, s String,
        c1 String, c2 String, c3 String, c4 String, c5 String, c6 Int64, c7 Int64, c8 Int64, c9 Int64, c10 Int64,
        t Tuple(c1 String, c2 String, c3 String, c4 String, c5 String, c6 Int64, c7 Int64, c8 Int64, c9 Int64, c10 Int64))
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

# Eight data files: one INSERT writes one file with one insert thread.
for i in {0..7}; do
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --max_insert_threads=1 --query "
        INSERT INTO ${TABLE}
        SELECT number AS k, cityHash64(number) % 1000003 AS ts, concat('val_', toString(number)) AS s,
            toString(number) AS c1, concat('a', c1) AS c2, concat('b', c1) AS c3, concat('c', c1) AS c4, concat('d', c1) AS c5,
            k * 6 AS c6, k * 7 AS c7, k * 8 AS c8, k * 9 AS c9, k * 10 AS c10, (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) AS t
        FROM numbers($((i * 2000)), 2000)
    "
done

# The main pass reads `ts` and `k`; the lazy pass reads 1 leaf column (narrow), 10 (wide) or one Tuple of 10 (nested).
NARROW="SELECT k, s FROM ${TABLE} ORDER BY ts, k LIMIT 3000"
WIDE="SELECT k, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10 FROM ${TABLE} ORDER BY ts, k LIMIT 100"
NESTED="SELECT k, t FROM ${TABLE} ORDER BY ts, k LIMIT 100"

# No limit on the LIMIT of lazy materialization; `max_threads` is not lowered when the server is short of free memory.
LAZY_SETTINGS=(--query_plan_optimize_lazy_materialization=1 --query_plan_max_limit_for_lazy_materialization=0 --max_threads_min_free_memory_per_thread=0)

echo '-- rows and data files of the LIMITs'
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(_file) FROM (SELECT k, _file FROM ${TABLE} ORDER BY ts, k LIMIT 3000)"
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(_file) FROM (SELECT k, _file FROM ${TABLE} ORDER BY ts, k LIMIT 100)"

echo '-- the lazy read step is in the plan'
${CLICKHOUSE_CLIENT} "${LAZY_SETTINGS[@]}" --query_plan_optimize_lazy_materialization_for_object_storage=1 \
    --query "EXPLAIN ${NARROW}" | grep -c 'LazilyReadFromObjectStorage'

echo '-- the same result with the optimization enabled and disabled'
same_result()
{
    local kind=$1 query=$2 threads=$3
    local results=()
    for enabled in 1 0; do
        # Small blocks: every reader returns many chunks, so the chunks of later groups arrive before earlier groups finish.
        results+=("$(${CLICKHOUSE_CLIENT} "${LAZY_SETTINGS[@]}" --max_threads="$threads" --max_block_size=7 \
            --query_plan_optimize_lazy_materialization_for_object_storage="$enabled" --query "$query" | md5sum)")
    done
    if [ "${results[0]}" = "${results[1]}" ]; then
        echo "$kind, max_threads = $threads: same"
    else
        echo "$kind, max_threads = $threads: different"
    fi
}
same_result narrow "$NARROW" 1
same_result narrow "$NARROW" 4
same_result narrow "$NARROW" 16
same_result wide "$WIDE" 16
same_result nested "$NESTED" 16

echo '-- the number of readers'
QUERY_ID_PREFIX="${CLICKHOUSE_DATABASE}_$(random_str 10)_readers"
for threads in 04 16; do
    for kind in narrow wide nested; do
        case $kind in
            narrow) query="$NARROW" ;;
            wide) query="$WIDE" ;;
            nested) query="$NESTED" ;;
        esac
        # The reader count is read from `system.processors_profile_log`.
        ${CLICKHOUSE_CLIENT} "${LAZY_SETTINGS[@]}" --query_plan_optimize_lazy_materialization_for_object_storage=1 \
            --log_processors_profiles=1 --max_threads="$((10#$threads))" --query_id="${QUERY_ID_PREFIX}_${threads}_${kind}" \
            --query "$query FORMAT Null"
    done
done
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS processors_profile_log"
# The readers carry the name of the processor that creates them.
${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(query_id, '${QUERY_ID_PREFIX}_', '') AS threads_kind, countIf(name = 'LazyReadFromObjectStorageSource') - 1 AS readers
    FROM system.processors_profile_log
    WHERE event_date >= yesterday() AND startsWith(query_id, '${QUERY_ID_PREFIX}_')
    GROUP BY query_id ORDER BY query_id
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
