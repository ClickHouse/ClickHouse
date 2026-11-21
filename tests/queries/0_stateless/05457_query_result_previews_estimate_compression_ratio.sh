#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The snapshot of a query result preview copies the live aggregate states through their serialization,
# so the serialization must not change the state the aggregation continues into: the serialization of
# `estimateCompressionRatio` used to finalize the live calculator, and the rows aggregated before the
# preview were lost from the final result.

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
URL="${URL}&framing_output_format=JSONEachPacketString&max_threads=1&max_block_size=65536"
PREVIEWS="&query_result_previews=1&query_result_previews_min_interval_ms=0&query_result_previews_max_result_bytes=0"
QUERY="SELECT estimateCompressionRatio('LZ4')(if(number < 65536, toUInt64(0), cityHash64(number))) FROM numbers(262144) FORMAT JSONCompactEachRow"

WITH_PREVIEWS_RESPONSE=$(${CLICKHOUSE_CURL} -sS "${URL}${PREVIEWS}" -d "${QUERY}")
PREVIEWS_COUNT=$(echo "${WITH_PREVIEWS_RESPONSE}" | grep -c '"packet":"preview"')
if [ "${PREVIEWS_COUNT}" -ge 1 ]; then echo "has previews"; else echo "no previews: ${WITH_PREVIEWS_RESPONSE}"; fi

WITH_PREVIEWS=$(echo "${WITH_PREVIEWS_RESPONSE}" | grep '"packet":"data"')
WITHOUT_PREVIEWS=$(${CLICKHOUSE_CURL} -sS "${URL}&query_result_previews=0" -d "${QUERY}" | grep '"packet":"data"')
if [ "${WITH_PREVIEWS}" == "${WITHOUT_PREVIEWS}" ]; then echo "identical"; else echo "differ: ${WITH_PREVIEWS} vs ${WITHOUT_PREVIEWS}"; fi
