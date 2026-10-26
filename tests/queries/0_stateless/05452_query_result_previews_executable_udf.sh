#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An executable UDF runs an external program, which can perform external actions and keep state
# between invocations in a pool of reused processes. Evaluating it on the intermediate values of a
# query result preview would repeat such actions, so the previews stay dormant for an expression
# with an executable UDF, while the same query with a regular function emits previews.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
URL="${URL}&framing_output_format=JSONEachPacketString&query_result_previews=1&query_result_previews_min_interval_ms=0"
# Pin the settings that determine the number of source blocks, so that enough blocks are consumed
# by the aggregation to emit previews regardless of the settings randomization in CI.
URL="${URL}&max_block_size=65536&max_threads=1&group_by_two_level_threshold=100000000&group_by_two_level_threshold_bytes=1000000000"
URL="${URL}&max_bytes_before_external_group_by=0&max_bytes_ratio_before_external_group_by=0&enable_adaptive_aggregator=0"

run()
{
    RESPONSE=$(${CLICKHOUSE_CURL} -sS "${URL}" -d "SELECT $1 FROM (SELECT count() AS c FROM numbers(1000000)) FORMAT JSONCompactEachRow")
    PREVIEWS_COUNT=$(echo "${RESPONSE}" | grep -c '"packet":"preview"')
    if [ "${PREVIEWS_COUNT}" -ge 1 ]; then echo "has previews"; else echo "no previews"; fi
    echo "${RESPONSE}" | grep -E '"packet":"data"|Exception'
}

run "plus(c, 1)"
run "test_function(c, 1)"
