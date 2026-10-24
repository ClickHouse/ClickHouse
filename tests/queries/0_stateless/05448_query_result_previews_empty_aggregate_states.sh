#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When all the aggregate states have zero size (an aggregate function of a `NULL` argument is `nothing`),
# the snapshot of a query result preview must not merge a state into itself: every zero-size
# allocation from the snapshot arena returns the same address.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
URL="${URL}&framing_output_format=JSONEachPacketString&query_result_previews=1&query_result_previews_min_interval_ms=0"
URL="${URL}&max_threads=2&max_block_size=1000"

run()
{
    RESPONSE=$(${CLICKHOUSE_CURL} -sS "${URL}" -d "$1 FORMAT JSONCompactEachRow")
    echo "${RESPONSE}" | grep -E '"packet":"data"|Exception'
}

run "SELECT toString(number % 3) AS k, uniqExactOrNull(number % NULL) AS u FROM numbers_mt(100000) GROUP BY k ORDER BY k"
run "SELECT number % 2 AS k, countOrDefault(NULL) AS c FROM numbers_mt(100000) GROUP BY k ORDER BY k"
run "SELECT uniqExactOrNull(number % NULL) FROM numbers_mt(100000)"
