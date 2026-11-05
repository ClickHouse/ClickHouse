#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When a function throws on a query result preview, the preview is dropped without pushing anything
# downstream. An empty chunk in its place would lose the preview marker and reach the main output
# format as a part of the result: `Npy` would take the shape of its array from the empty chunk and
# throw `ILLEGAL_COLUMN` on the real result.
#
# With `max_threads = 1` and one source block of 65536 rows per step, the first preview has `c = 32768`.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
URL="${URL}&framing_output_format=JSONEachPacketBase64&query_result_previews=1&query_result_previews_min_interval_ms=0"
URL="${URL}&max_block_size=65536&max_threads=1&group_by_two_level_threshold=100000000&group_by_two_level_threshold_bytes=1000000000"
URL="${URL}&max_bytes_before_external_group_by=0&max_bytes_ratio_before_external_group_by=0&enable_adaptive_aggregator=0"

RESPONSE=$(${CLICKHOUSE_CURL} -sS "${URL}" -d "SELECT [intDiv(1, c - 32768)] AS a FROM (SELECT countIf(number % 2 = 0) AS c FROM numbers(131072)) FORMAT Npy")
echo "previews: $(echo "${RESPONSE}" | grep -c '"packet":"preview"')"
echo "data: $(echo "${RESPONSE}" | grep -c '"packet":"data"')"
echo "errors: $(echo "${RESPONSE}" | grep -c -E 'Exception|ILLEGAL_COLUMN|ILLEGAL_DIVISION')"
