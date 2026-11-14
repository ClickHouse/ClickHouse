#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query result preview holds intermediate values, so a function downstream of the emitter may
# throw on a preview while it does not throw on the result. Such an error must not fail the query:
# the preview is dropped and the previews of its stage stop for the rest of the query.
#
# With `max_threads = 1` and one source block of 65536 rows per step, the aggregations below emit a
# preview after every block but the last one, with `c` growing by 32768.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
URL="${URL}&framing_output_format=JSONEachPacketString&query_result_previews=1&query_result_previews_min_interval_ms=0"
URL="${URL}&max_block_size=65536&max_threads=1&group_by_two_level_threshold=100000000&group_by_two_level_threshold_bytes=1000000000"
URL="${URL}&max_bytes_before_external_group_by=0&max_bytes_ratio_before_external_group_by=0&enable_adaptive_aggregator=0"

run()
{
    echo "--- $1"
    RESPONSE=$(${CLICKHOUSE_CURL} -sS "${URL}" -d "$2 FORMAT JSONCompactEachRow")
    echo "previews: $(echo "${RESPONSE}" | grep -c '"packet":"preview"')"
    echo "${RESPONSE}" | grep '"packet":"data"'
}

# The first preview has `c = 32768`, the second one `c = 65536` throws, the third one is not emitted.
run "an expression throws on the second preview" \
    "SELECT intDiv(1, c - 65536) AS x FROM (SELECT countIf(number % 2 = 0) AS c FROM numbers(262144))"

run "HAVING throws on the second preview" \
    "SELECT number % 2 AS k, count() AS c FROM numbers(262144) GROUP BY k HAVING intDiv(1, c - 65536) = 0 ORDER BY k"

# A position over 16777215 throws `TOO_LARGE_ARRAY_SIZE`: the first preview has `c = 32768`.
run "a window function throws on the first preview" \
    "SELECT k, length(groupArrayInsertAt(1, toUInt64(65536 - c) * 1000) OVER ()) AS l FROM (SELECT number % 2 AS k, count() AS c FROM numbers(131072) GROUP BY k) ORDER BY k"

echo "--- an error on the result still fails the query"
RESPONSE=$(${CLICKHOUSE_CURL} -sS "${URL}" -d "SELECT intDiv(1, c - 65536) AS x FROM (SELECT countIf(number % 2 = 0) AS c FROM numbers(131072)) FORMAT JSONCompactEachRow")
if echo "${RESPONSE}" | grep -q 'ILLEGAL_DIVISION'; then echo "fails"; else echo "does not fail: ${RESPONSE}"; fi
