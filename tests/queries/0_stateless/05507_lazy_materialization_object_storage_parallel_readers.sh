#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option) and MinIO
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
ARCHIVE_PATH="${USER_FILES_PATH}/${TABLE}_archive"

trap 'rm -rf "${TABLE_PATH}" "${ARCHIVE_PATH}" 2>/dev/null' EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (
        k Int64, ts Int64, s String,
        c1 String, c2 String, c3 String, c4 String, c5 String, c6 Int64, c7 Int64, c8 Int64, c9 Int64, c10 Int64,
        t Tuple(c1 String, c2 String, c3 String, c4 String, c5 String, c6 Int64, c7 Int64, c8 Int64, c9 Int64, c10 Int64),
        a Array(Tuple(c1 String, c2 String, c3 String, c4 String, c5 String, c6 Int64, c7 Int64, c8 Int64, c9 Int64, c10 Int64)),
        m Map(String, Tuple(c1 String, c2 String, c3 String, c4 String, c5 String, c6 Int64, c7 Int64, c8 Int64, c9 Int64, c10 Int64)))
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

# Eight data files: one INSERT writes one file with one insert thread.
for i in {0..7}; do
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --max_insert_threads=1 --query "
        INSERT INTO ${TABLE}
        SELECT number AS k, cityHash64(number) % 1000003 AS ts, concat('val_', toString(number)) AS s,
            toString(number) AS c1, concat('a', c1) AS c2, concat('b', c1) AS c3, concat('c', c1) AS c4, concat('d', c1) AS c5,
            k * 6 AS c6, k * 7 AS c7, k * 8 AS c8, k * 9 AS c9, k * 10 AS c10, (c1, c2, c3, c4, c5, c6, c7, c8, c9, c10) AS t,
            [t] AS a, map(c1, t) AS m
        FROM numbers($((i * 2000)), 2000)
    "
done

# The main pass reads `ts` and `k` of 8 files, so with at most 8 readers; the lazy pass reads 1 leaf column (narrow),
# 10 (wide), one Tuple of 10 (nested), an Array of that Tuple (10) or a Map with that Tuple as values (11).
NARROW="SELECT k, s FROM ${TABLE} ORDER BY ts, k LIMIT 3000"
WIDE="SELECT k, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10 FROM ${TABLE} ORDER BY ts, k LIMIT 100"
NESTED="SELECT k, t FROM ${TABLE} ORDER BY ts, k LIMIT 100"
ARRAY="SELECT k, a FROM ${TABLE} ORDER BY ts, k LIMIT 100"
MAP="SELECT k, m FROM ${TABLE} ORDER BY ts, k LIMIT 100"

# No limit on the LIMIT of lazy materialization; `max_threads` is lowered neither when the server is short of free memory
# nor to the fair share of CPU slots of a busy server (`max_threads = 16` is `auto` on a server with 16 cores).
LAZY_SETTINGS=(--query_plan_optimize_lazy_materialization=1 --query_plan_max_limit_for_lazy_materialization=0 --max_threads_min_free_memory_per_thread=0
    --use_concurrency_control_for_max_threads=0)

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

QUERY_ID_PREFIX="${CLICKHOUSE_DATABASE}_$(random_str 10)_readers"
# The reader count is read from `system.processors_profile_log`.
run_counted()
{
    local name=$1 threads=$2 query=$3
    shift 3
    ${CLICKHOUSE_CLIENT} "${LAZY_SETTINGS[@]}" --query_plan_optimize_lazy_materialization_for_object_storage=1 \
        --log_processors_profiles=1 --max_threads="$threads" --query_id="${QUERY_ID_PREFIX}_${name}" "$@" \
        --query "$query FORMAT Null"
}
for threads in 04 16; do
    run_counted "${threads}_narrow" "$((10#$threads))" "$NARROW"
    run_counted "${threads}_wide" "$((10#$threads))" "$WIDE"
    run_counted "${threads}_nested" "$((10#$threads))" "$NESTED"
done
run_counted 16_array 16 "$ARRAY"
run_counted 16_map 16 "$MAP"
# The ratio lowers the stream budget of the main read as `max_streams_for_files_processing_in_cluster_functions` does in
# `*Cluster` functions (both arrive as the `num_streams` of the read step), so no cluster is needed.
run_counted 16_narrow_streams 16 "$NARROW" --max_streams_to_max_threads_ratio=0.25

# The leaf columns of a `JSON` or `Dynamic` column depend on the file, so their lazy read keeps one reader:
# a Parquet struct read as `JSON`, a Parquet string read as `Dynamic`.
echo '-- the lazy read step is in the plan of a JSON column of Parquet files'
S3_URL="http://localhost:11111/test/${CLICKHOUSE_DATABASE}_lazy_parallel"
for i in {0..7}; do
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION s3('${S3_URL}/data_${i}.parquet', 'test', 'testtest', 'Parquet',
            'k Int64, ts Int64, s String, j Tuple(c1 String, c2 String, c3 String, c4 String, c5 String)')
        SELECT number AS k, cityHash64(number) % 1000003 AS ts, concat('val_', toString(number)) AS s,
            (toString(number), concat('a', toString(number)), concat('b', toString(number)), concat('c', toString(number)),
                concat('d', toString(number))) AS j
        FROM numbers($((i * 2000)), 2000)
        SETTINGS s3_truncate_on_insert = 1
    "
done
S3_JSON="SELECT k, j FROM s3('${S3_URL}/data_*.parquet', 'test', 'testtest', 'Parquet', 'k Int64, ts Int64, j JSON') ORDER BY ts, k LIMIT 100"
S3_DYNAMIC="SELECT k, s FROM s3('${S3_URL}/data_*.parquet', 'test', 'testtest', 'Parquet', 'k Int64, ts Int64, s Dynamic') ORDER BY ts, k LIMIT 100"
# See 04839_parquet_lazy_materialization_object_storage_json_subcolumn for `s3_validate_etag_on_read`.
${CLICKHOUSE_CLIENT} "${LAZY_SETTINGS[@]}" --query_plan_optimize_lazy_materialization_for_object_storage=1 --s3_validate_etag_on_read=1 \
    --max_threads=16 --query "EXPLAIN ${S3_JSON}" | grep -c 'LazilyReadFromObjectStorage'
run_counted 16_json 16 "$S3_JSON" --s3_validate_etag_on_read=1
run_counted 16_dynamic 16 "$S3_DYNAMIC" --s3_validate_etag_on_read=1
# The same struct read as `Nullable(Tuple)` counts as its 5 elements (`IcebergLocal` stores a `Nullable(Tuple)` as a `Tuple`).
S3_NULLABLE="SELECT k, j FROM s3('${S3_URL}/data_*.parquet', 'test', 'testtest', 'Parquet', 'k Int64, ts Int64, j Nullable(Tuple(c1 String, c2 String, c3 String, c4 String, c5 String))') ORDER BY ts, k LIMIT 100"
run_counted 16_nullable 16 "$S3_NULLABLE" --s3_validate_etag_on_read=1
# A `Dynamic` element makes the width of the whole struct unknown.
S3_NESTED_DYNAMIC="SELECT k, j FROM s3('${S3_URL}/data_*.parquet', 'test', 'testtest', 'Parquet', 'k Int64, ts Int64, j Tuple(c1 Dynamic, c2 String, c3 String, c4 String, c5 String)') ORDER BY ts, k LIMIT 100"
run_counted 16_nested_dynamic 16 "$S3_NESTED_DYNAMIC" --s3_validate_etag_on_read=1
# With `aggregate_function_input_format = 'value'` the format reads the values of the states: the struct of 5 elements.
S3_AGGREGATE="SELECT k, j FROM s3('${S3_URL}/data_*.parquet', 'test', 'testtest', 'Parquet', 'k Int64, ts Int64, j AggregateFunction(any, Tuple(c1 String, c2 String, c3 String, c4 String, c5 String))') ORDER BY ts, k LIMIT 100"
run_counted 16_aggregate_value 16 "$S3_AGGREGATE" --s3_validate_etag_on_read=1 --aggregate_function_input_format=value
# The main pass reads a tar of 8 files with one source, so the lazy pass reads 1 leaf column with 2 readers.
for i in {0..7}; do
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION file('${TABLE}_archive/data_${i}.parquet', 'Parquet', 'k Int64, ts Int64, s String')
        SELECT number AS k, cityHash64(number) % 1000003 AS ts, concat('val_', toString(number)) AS s FROM numbers($((i * 2000)), 2000)
    "
done
tar cf "${ARCHIVE_PATH}/data.tar" -C "${ARCHIVE_PATH}" data_0.parquet data_1.parquet data_2.parquet data_3.parquet \
    data_4.parquet data_5.parquet data_6.parquet data_7.parquet
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3('${S3_URL}/data.tar', 'test', 'testtest', 'RawBLOB')
    SELECT * FROM file('${TABLE}_archive/data.tar', 'RawBLOB') SETTINGS s3_truncate_on_insert = 1
"
S3_ARCHIVE="SELECT k, s FROM s3('${S3_URL}/data.tar :: *.parquet', 'test', 'testtest', 'Parquet', 'k Int64, ts Int64, s String') ORDER BY ts, k LIMIT 100"
run_counted 16_archive 16 "$S3_ARCHIVE" --s3_validate_etag_on_read=1

echo '-- the number of readers'
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS processors_profile_log"
# The readers carry the name of the processor that creates them.
${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(query_id, '${QUERY_ID_PREFIX}_', '') AS threads_kind, countIf(name = 'LazyReadFromObjectStorageSource') - 1 AS readers
    FROM system.processors_profile_log
    WHERE event_date >= yesterday() AND startsWith(query_id, '${QUERY_ID_PREFIX}_')
    GROUP BY query_id ORDER BY query_id
"

# The files are split by rows, so no reader of the four gets more than twice its share (the readers have no input rows).
echo '-- the rows of the readers are balanced'
${CLICKHOUSE_CLIENT} --query "
    SELECT max(output_rows) * count() <= 2 * sum(output_rows)
    FROM system.processors_profile_log
    WHERE event_date >= yesterday() AND query_id = '${QUERY_ID_PREFIX}_04_narrow' AND name = 'LazyReadFromObjectStorageSource' AND input_rows = 0
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
