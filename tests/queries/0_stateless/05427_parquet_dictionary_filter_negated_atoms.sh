#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format which is not built in fasttest.

# The Parquet dictionary filter can only rule out `x = c`, so a row group's dictionary must be read for `x != c` or
# `x NOT IN (...)` only when a `NOT` is above them, and conditions on other atoms must still prune row groups.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

# 10 dictionary-encoded row groups of 1000 rows without bloom filters: each holds `s` in {a, b}, `flag` in {0, 1}, `h = 1`.
${CLICKHOUSE_LOCAL} --query="
    INSERT INTO FUNCTION file('${DATA_FILE}', Parquet)
    SELECT if(number % 2 = 0, 'a', 'b') AS s, toUInt8(number % 2) AS flag, toUInt8(1) AS h
    FROM numbers(10000)
    SETTINGS output_format_parquet_row_group_size = 1000, output_format_parquet_write_bloom_filter = 0,
        engine_file_truncate_on_insert = 1, max_block_size = 1000, max_insert_threads = 1, max_threads = 1"

SETTINGS="input_format_parquet_filter_push_down = 0, input_format_parquet_page_filter_push_down = 0, input_format_parquet_bloom_filter_push_down = 0, optimize_move_to_prewhere = 0, use_cache_for_count_from_files = 0"

query() {
    echo "SELECT count() FROM file('${DATA_FILE}', Parquet, 's String, flag UInt8, h UInt8') WHERE $1 SETTINGS ${SETTINGS}, input_format_parquet_dictionary_filter_push_down = $2"
}

decoding_tasks() {
    ${CLICKHOUSE_LOCAL} --print-profile-events --profile-events-delay-ms=-1 --query="$(query "$1" "$2") FORMAT Null" 2>&1 \
        | awk '/ParquetDecodingTasks:/ { n += $(NF-1) } END { print n + 0 }'
}

# No row group is pruned here, so the extra decoding tasks with the dictionary filter are the dictionaries read.
echo "dictionaries read:"
for condition in "s = 'a'" "s != ''" "s NOT IN ('a', 'x')" "flag" "NOT flag" "NOT flag AND s != ''"; do
    echo "${condition}: $(( $(decoding_tasks "${condition}" 1048576) - $(decoding_tasks "${condition}" 0) ))"
done

echo "row groups pruned by the dictionary filter:"
for condition in "s != '' AND s = 'absent'" "NOT h"; do
    echo "${condition}: $(${CLICKHOUSE_LOCAL} --query="$(query "${condition}" 1048576) FORMAT JSON" | jq -c '{count: .data[0]["count()"], rows_read: .statistics.rows_read}')"
done

rm -f "${DATA_FILE}"
