#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-parallel
# no-fasttest: requires an ORC-enabled build.
# no-random-settings: the local read method and cache settings are the regression.
# no-parallel: the test clears the process-wide ORC metadata cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

file_name="${CLICKHOUSE_DATABASE}_05877.orc"
user_files_path=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.server_settings WHERE name = 'user_files_path'")
data_file="${user_files_path}/${file_name}"

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${file_name}', ORC, 'id UInt64, payload String') SELECT number, repeat('x', 100) AS payload FROM numbers(1000000) SETTINGS engine_file_truncate_on_insert = 1, output_format_orc_compression_method = 'none'"
file_size=$(stat -f %z "${data_file}")
if [[ ${file_size} -le 65536 ]]; then
    echo "fixture too small: ${file_size}"
    exit 1
fi

# `ReadBufferFromFile` has seek + read but no positional readBigAt. It must not
# be treated as non-seekable and copied in full just to obtain a footer count.
read_count() {
    local enabled=$1
    local query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${enabled}_$RANDOM$RANDOM"
    ${CLICKHOUSE_CLIENT} --query_id="${query_id}" --query "SELECT count() FROM file('${file_name}', ORC) SETTINGS storage_file_read_method = 'read', use_orc_metadata_cache = ${enabled}, use_cache_for_count_from_files = 0, optimize_count_from_files = 0, log_queries = 1, max_threads = 1"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    bytes_read=$(${CLICKHOUSE_CLIENT} --query "SELECT ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] FROM system.query_log WHERE query_id = '${query_id}' AND type = 'QueryFinish' AND current_database = currentDatabase() ORDER BY event_time_microseconds DESC LIMIT 1")
    if [[ ${bytes_read} -ge ${file_size} ]]; then
        echo "whole file read: ${bytes_read} of ${file_size}"
        return 1
    fi
    echo "bounded"
}

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP ORC METADATA CACHE"
read_count 0
read_count 1
read_count 1
