#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel-replicas: profile events are not available on the second replica

# ORDER BY ... LIMIT over a Parquet file with many row groups, with the row groups served from the columns cache:
# the top-K threshold is checked again when a row group is delivered, which must not lose rows of the result.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet)
    SELECT intDiv(sipHash64(number) % 1000000, 7) AS v, toString(number) AS s
    FROM numbers(1000000)
    SETTINGS output_format_parquet_row_group_size = 10000, max_block_size = 10000, engine_file_truncate_on_insert = 1"
# Only a file that has not been modified for a few seconds is cached: its version token identifies its content only then.
touch -d '1 minute ago' "${FILE_PATH}"

SETTINGS="max_threads = 16, max_parsing_threads = 16, use_top_k_dynamic_filtering = 1, input_format_parquet_use_native_reader_v3 = 1"
CACHE_SETTINGS="use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1"

function run()
{
    # $1 - log comment, $2 - ORDER BY, $3 - settings.
    $CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_$1" -q "
        SELECT v, s FROM file('${FILE}') ORDER BY $2 LIMIT 5 SETTINGS ${SETTINGS}, $3"
}

echo "without the cache"
run nocache_asc "v, s" "use_columns_cache = 0"
run nocache_desc "v DESC, s" "use_columns_cache = 0"

# Populate the columns cache with both columns of every row group.
$CLICKHOUSE_CLIENT -q "SELECT sum(v), sum(length(s)) FROM file('${FILE}') SETTINGS ${SETTINGS}, ${CACHE_SETTINGS}" > /dev/null

echo "with the cache, three times"
for i in 1 2 3; do run "asc_$i" "v, s" "${CACHE_SETTINGS}"; done
for i in 1 2 3; do run "desc_$i" "v DESC, s" "${CACHE_SETTINGS}"; done

echo "the warm reads hit the columns cache"
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT replaceOne(log_comment, '${CLICKHOUSE_TEST_UNIQUE_NAME}_', '') AS name, ProfileEvents['ColumnsCacheHits'] > 0
    FROM system.query_log
    WHERE log_comment LIKE '${CLICKHOUSE_TEST_UNIQUE_NAME}\_%' AND log_comment NOT LIKE '%nocache%'
        AND type = 'QueryFinish' AND current_database = currentDatabase()
    ORDER BY name"

rm -f "${FILE_PATH}"
