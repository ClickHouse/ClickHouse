#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query result preview holds intermediate values, which may be invalid for the finalization of an
# aggregate function or for the output format, while the result of the query is valid. Such an error
# must not fail the query: the preview is dropped and the previews of its stage stop.
#
# With `max_threads = 1` and one source block of 65536 rows per step, the aggregations below emit a
# preview after the first block.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
URL="${URL}&query_result_previews_min_interval_ms=0&max_block_size=65536&max_threads=1"
URL="${URL}&group_by_two_level_threshold=100000000&group_by_two_level_threshold_bytes=1000000000"
URL="${URL}&max_bytes_before_external_group_by=0&max_bytes_ratio_before_external_group_by=0&enable_adaptive_aggregator=0"

echo "--- the finalization of an aggregate function throws on the first preview"
# The first block holds only the first sample, and `kolmogorovSmirnovTest` throws on an empty sample.
QUERY="SELECT kolmogorovSmirnovTest(number, toUInt8(number >= 65536)) AS r FROM numbers(131072) FORMAT JSONCompactEachRow"
RESPONSE=$(${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&query_result_previews=1" -d "${QUERY}")
echo "previews: $(echo "${RESPONSE}" | grep -c '"packet":"preview"')"
echo "exceptions: $(echo "${RESPONSE}" | grep -c '"packet":"exception"')"
WITH_PREVIEWS=$(echo "${RESPONSE}" | grep '"packet":"data"')
WITHOUT_PREVIEWS=$(${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketString&query_result_previews=0" -d "${QUERY}" | grep '"packet":"data"')
[ -n "${WITH_PREVIEWS}" ] && [ "${WITH_PREVIEWS}" = "${WITHOUT_PREVIEWS}" ] && echo "same result" || echo "different results: ${WITH_PREVIEWS} and ${WITHOUT_PREVIEWS}"

echo "--- the output format rejects the first preview"
# The first preview has `c = 32768`, so its array is empty, and `Npy` rejects a zero-sized dimension.
QUERY="SELECT arrayResize([toUInt8(0)], toUInt64(c > 32768)) AS a FROM (SELECT countIf(number % 2 = 0) AS c FROM numbers(131072)) FORMAT Npy"
RESPONSE=$(${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketBase64&query_result_previews=1" -d "${QUERY}")
echo "previews: $(echo "${RESPONSE}" | grep -c '"packet":"preview"')"
echo "exceptions: $(echo "${RESPONSE}" | grep -c '"packet":"exception"')"
WITH_PREVIEWS=$(echo "${RESPONSE}" | grep '"packet":"data"')
WITHOUT_PREVIEWS=$(${CLICKHOUSE_CURL} -sS "${URL}&framing_output_format=JSONEachPacketBase64&query_result_previews=0" -d "${QUERY}" | grep '"packet":"data"')
[ -n "${WITH_PREVIEWS}" ] && [ "${WITH_PREVIEWS}" = "${WITHOUT_PREVIEWS}" ] && echo "same result" || echo "different results: ${WITH_PREVIEWS} and ${WITHOUT_PREVIEWS}"
