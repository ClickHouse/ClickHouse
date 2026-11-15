#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

# The Parquet dictionary and bloom filters must not skip a row group whose NULL rows match an `IN` set holding NULL (`transform_null_in = 1`) or a `has` array holding NULL.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# Two row groups of 100 rows. Row group 0: id {NULL (row 0), -1, 10}, s {NULL (row 1), 'a', 'c'}; row group 1: id {4, 6}, s {'x', 'z'}.
${CLICKHOUSE_CLIENT} --query="
    INSERT INTO FUNCTION file('${DATA_FILE}', Parquet)
    SELECT multiIf(number = 0, NULL, number < 100, if(number % 2 = 0, -1, 10), if(number % 2 = 0, 4, 6))::Nullable(Int64) AS id,
           multiIf(number = 1, NULL, number < 100, if(number % 2 = 0, 'a', 'c'), if(number % 2 = 0, 'x', 'z'))::Nullable(String) AS s
    FROM numbers(200)
    SETTINGS output_format_parquet_row_group_size = 100, output_format_parquet_write_bloom_filter = 1,
        engine_file_truncate_on_insert = 1, max_block_size = 1000000, max_threads = 1"

CH="${CLICKHOUSE_CLIENT} --use_query_condition_cache=0 --optimize_move_to_prewhere=0 --use_cache_for_count_from_files=0"

run() {
    for filter in dictionary bloom; do
        if [ "${filter}" = dictionary ]; then
            FLAGS="--input_format_parquet_bloom_filter_push_down=0 --input_format_parquet_dictionary_filter_push_down=1048576"
        else
            FLAGS="--input_format_parquet_bloom_filter_push_down=1 --input_format_parquet_dictionary_filter_push_down=0"
        fi
        echo -n "$1, ${filter}: "
        ${CH} --transform_null_in="${2:-1}" ${FLAGS} --query="SELECT count() FROM file('${DATA_FILE}', Parquet) WHERE $1 FORMAT JSON" \
            | jq -c '{c: .data[0]["count()"], rows_read: .statistics.rows_read}'
    done
}

run "id IN (NULL, 5)"
run "s IN (NULL, 'b')"
run "(id, s) IN ((NULL, 'a'), (5, 'y'))"
run "id IN (5)"
run "has([NULL, 5]::Array(Nullable(Int64)), id)" 0
