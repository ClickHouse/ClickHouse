#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The snapshot of a query result preview must not share the storage of an aggregate state with the
# aggregation, which keeps inserting into it: an empty `uniqExact` adopts a two-level set (over
# 100000 values) when merged, instead of copying it. With several threads, the preview snapshots
# are taken while the participants keep inserting past that size. Thread Sanitizer catches the race.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
URL="${URL}&framing_output_format=JSONEachPacketString&query_result_previews=1&query_result_previews_min_interval_ms=0"
URL="${URL}&query_result_previews_max_result_bytes=0&max_threads=4&max_block_size=8192"

run()
{
    RESPONSE=$(${CLICKHOUSE_CURL} -sS "${URL}" -d "$1 FORMAT JSONCompactEachRow")
    echo "${RESPONSE}" | grep -E '"packet":"data"|Exception'
}

run "SELECT uniqExact(number), finalizeAggregation(uniqExactState(number)) FROM numbers_mt(1000000)"
run "SELECT number % 2 AS k, uniqExact(number) FROM numbers_mt(1000000) GROUP BY k ORDER BY k"
