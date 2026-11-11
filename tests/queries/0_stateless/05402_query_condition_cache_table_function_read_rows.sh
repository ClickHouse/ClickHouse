#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-parallel-replicas
# Tag no-fasttest: needs Parquet
# Tag no-parallel: relies on the entries of the instance-wide query condition cache, which a parallel
# sibling test can wipe at any moment (see 04498_query_condition_cache_local_files).
# Tag no-parallel-replicas: with parallel replicas the replicas read the file.

# The query condition cache saves reads through a table function without a UUID. All values are even, so
# the odd value 3 never matches, but it is within the range of the statistics of the first row group:
# without the cache the reader has to read and decode that row group to find out. A repeated query with
# the cache knows that no row group of this version of the file matches, and does not read the file at all.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="${CLICKHOUSE_DATABASE}_05402"
mkdir -p "${USER_FILES_PATH:?}/${DATA_DIR}"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DATA_DIR}/data.parquet') SELECT number * 2 AS b FROM numbers(1000000)
    SETTINGS output_format_parquet_row_group_size = 100000, engine_file_truncate_on_insert = 1"
# Backdate the file so its version token has settled and the cache engages.
touch -d '2020-01-01 00:00:00' "${USER_FILES_PATH}/${DATA_DIR}/data.parquet"

QUERY="SELECT count() FROM file('${DATA_DIR}/data.parquet') WHERE b = 3"
SETTINGS="enable_analyzer = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, max_threads = 1, storage_file_read_method = 'pread'"

${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_no_cache" --query "${QUERY} SETTINGS ${SETTINGS}, use_query_condition_cache = 0"
${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_populate" --query "${QUERY} SETTINGS ${SETTINGS}, use_query_condition_cache = 1"
${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_cached" --query "${QUERY} SETTINGS ${SETTINGS}, use_query_condition_cache = 1"

# Per query: whether it hit the cache, the number of row groups it read, and whether it read from the file.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
for name in no_cache populate cached
do
    ${CLICKHOUSE_CLIENT} --query "
        SELECT '${name}', ProfileEvents['QueryConditionCacheHits'] > 0, ProfileEvents['ParquetReadRowGroups'],
            ProfileEvents['ReadBufferFromFileDescriptorReadBytes'] > 0
        FROM system.query_log
        WHERE query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}' AND current_database = currentDatabase() AND type = 'QueryFinish'"
done

rm -rf "${USER_FILES_PATH:?}/${DATA_DIR}"
