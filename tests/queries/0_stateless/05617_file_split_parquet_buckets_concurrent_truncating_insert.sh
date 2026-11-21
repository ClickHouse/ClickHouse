#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A single Parquet file of a `File` table is read by several sources in parallel, one per bucket of its row
# groups. Every source must hold the shared lock of the table while it reads, so that a concurrent truncating
# `INSERT` waits for the read to finish instead of rewriting the file under it. The old analyzer destroys the
# query plan before the pipeline runs, so the lock must be held by the sources themselves and not by the plan.

DIR="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${DIR}"
mkdir -p "${DIR}"
chmod 777 "${DIR}"

# The format settings of a `File` table are fixed at `CREATE`: every file has a row group per 1000 rows.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE test (x UInt64) ENGINE = File(Parquet, '${DIR}/data.parquet') SETTINGS output_format_parquet_row_group_size = 1000"
${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(20000) SETTINGS engine_file_truncate_on_insert = 1"

READ_SETTINGS="enable_analyzer = 0, max_threads = 4, parallelize_output_from_storages = 1, input_format_parquet_preserve_order = 0, use_query_condition_cache = 0"

# The file is read by several sources.
${CLICKHOUSE_CLIENT} --query "EXPLAIN PIPELINE SELECT sum(x) FROM test SETTINGS ${READ_SETTINGS}" | grep -c 'File × 4'

function write_thread()
{
    for _ in $(seq 1 15)
    do
        ${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(10000) SETTINGS engine_file_truncate_on_insert = 1"
        ${CLICKHOUSE_CLIENT} --query "INSERT INTO test SELECT number FROM numbers(20000) SETTINGS engine_file_truncate_on_insert = 1"
    done
}

function read_thread()
{
    while [ ! -f "${DIR}/writer_finished" ]
    do
        # An error of the `SELECT` goes to the output and fails the test.
        result=$(${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM test SETTINGS ${READ_SETTINGS}" 2>&1)
        if [ "${result}" != "$(printf '10000\t49995000')" ] && [ "${result}" != "$(printf '20000\t199990000')" ]
        then
            echo "Unexpected result: ${result}"
        fi
    done
}

write_thread &
WRITER_PID=$!
read_thread &
read_thread &
wait "${WRITER_PID}"
touch "${DIR}/writer_finished"
wait

${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM test SETTINGS ${READ_SETTINGS}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE test"
rm -rf "${DIR}"
