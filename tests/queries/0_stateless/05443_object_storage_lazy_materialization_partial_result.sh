#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option) and Parquet
# - no-parallel-replicas: asserts on the query plan, which with parallel replicas reads the table with `ReadFromCluster`

# With `partial_result_on_first_cancel`, the first cancel stops the main sources, but the rows already read
# are still sorted, and the deferred columns of the selected rows must still be read by the lazy branch
# of lazy materialization of object storage and `file` tables, whose readers are created after the cancellation.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_lazy_partial"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
FILES_DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}"

trap 'rm -rf "${TABLE_PATH}" "${USER_FILES_PATH}/${FILES_DIR}" 2>/dev/null' EXIT

SETTINGS="max_threads = 1, max_block_size = 1000, input_format_parquet_max_block_size = 1000, use_top_k_dynamic_filtering = 0,
          query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000,
          query_plan_optimize_lazy_materialization_for_object_storage = 1, query_plan_optimize_lazy_materialization_for_file = 1,
          query_plan_execute_functions_after_sorting = 0, function_sleep_max_microseconds_per_block = 100000000,
          enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0"

# Runs the query, cancels it once it has read at least $3 rows and prints the partial result.
function run_cancelled()
{
    local query="$1"
    local query_id="${CLICKHOUSE_DATABASE}_lazy_partial_$2"
    local min_read_rows="$3"
    local client_out="${CLICKHOUSE_TMP}/lazy_partial_$2.out"
    local client_err="${CLICKHOUSE_TMP}/lazy_partial_$2.err"

    # The test is meaningful only if `payload` is deferred.
    $CLICKHOUSE_CLIENT -q "
        SELECT count() > 0 FROM (EXPLAIN actions = 1 $query SETTINGS $SETTINGS)
        WHERE explain LIKE '%Lazily read columns: payload%'"

    $CLICKHOUSE_CLIENT --query_id="$query_id" -q "
        $query SETTINGS $SETTINGS, partial_result_on_first_cancel = 1, interactive_delay = 1000" > "$client_out" 2> "$client_err" &
    local client_pid=$!

    local i=0
    while [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id' AND read_rows >= $min_read_rows")" -lt 1 ]; do
        sleep 0.3
        i=$((i + 1))
        if [ "$i" -gt 200 ]; then
            echo "Query did not start in time" >&2
            kill -9 $client_pid 2>/dev/null
            exit 1
        fi
    done

    kill -INT $client_pid
    wait $client_pid

    cat "$client_out"
    if grep -q -F "Exception" "$client_err"; then
        cat "$client_err"
    fi

    rm -f "$client_out" "$client_err"
}

# Object storage: an Iceberg table with one data file. Its reader takes the Parquet block size from the table settings.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE ${TABLE} (k Int64, payload String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    SETTINGS input_format_parquet_max_block_size = 1000;
    INSERT INTO ${TABLE} SELECT number, toString(number) FROM numbers(1000000)
    SETTINGS allow_insert_into_iceberg = 1, max_threads = 1, max_block_size = 1000, max_insert_threads = 1;"

run_cancelled "SELECT payload FROM ${TABLE} ORDER BY k + sleepEachRow(0.0001) LIMIT 5" iceberg 1

$CLICKHOUSE_CLIENT -q "DROP TABLE ${TABLE}"

# `file`: the selected rows are in five small files, so the lazy branch reads five chunks, and a large last file
# keeps the main branch busy. The list of alternatives (unlike `*`) keeps the order in which the files are read.
for i in 0 1 2 3 4; do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION file('${FILES_DIR}/f$i.parquet', Parquet, 'k Int64, payload String')
        SELECT $i + 5 * number, toString($i + 5 * number) FROM numbers(2000) SETTINGS max_threads = 1, max_block_size = 1000"
done
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILES_DIR}/f5.parquet', Parquet, 'k Int64, payload String')
    SELECT 10000000 + number, toString(10000000 + number) FROM numbers(1000000) SETTINGS max_threads = 1, max_block_size = 1000"

run_cancelled "SELECT payload FROM file('${FILES_DIR}/f{0,1,2,3,4,5}.parquet', Parquet, 'k Int64, payload String')
    ORDER BY k + sleepEachRow(0.0001) LIMIT 5" file 10000
