#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A query result preview of a window function is computed by a one-shot `WindowTransform` that is not
# a part of the pipeline, so the executor does not cancel it. It must observe the cancellation of the
# transform in the pipeline: otherwise `KILL QUERY` waits until the whole preview is computed, which
# takes very long with a costly frame.

QUERY_ID="${CLICKHOUSE_DATABASE}_window_preview_cancellation"

URL="${CLICKHOUSE_URL}&http_wait_end_of_query=0&http_response_buffer_size=0&output_format_parallel_formatting=0"
URL="${URL}&framing_output_format=JSONEachPacketString&query_result_previews=1&query_result_previews_min_interval_ms=0"
URL="${URL}&query_result_previews_max_result_rows=100000&max_threads=1&max_block_size=65536"
URL="${URL}&group_by_two_level_threshold=0&group_by_two_level_threshold_bytes=0&query_id=${QUERY_ID}"

${CLICKHOUSE_CURL} -sS "${URL}" -d "
    SELECT quantileExact(c) OVER (ORDER BY k ROWS BETWEEN 50000 PRECEDING AND CURRENT ROW)
    FROM
    (
        SELECT number % 65536 AS k, countIf(number % 2 = 0) AS c
        FROM numbers(10000000)
        GROUP BY k
    )
    FORMAT JSONCompactEachRow" > /dev/null 2>&1 &
QUERY_PID=$!

# Wait until the query is busy with the window functions: the aggregation alone takes a fraction of a second.
while kill -0 "${QUERY_PID}" 2>/dev/null && [[ $(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '${QUERY_ID}' AND elapsed > 1") != 1 ]]
do
    sleep 0.1
done

${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '${QUERY_ID}' SYNC FORMAT Null"
wait

${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '${QUERY_ID}'"
