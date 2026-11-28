#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A browser only reads a response header that the server lists in
# Access-Control-Expose-Headers. The HTTP interface writes X-ClickHouse-Progress,
# so the exposed list has to name it, like it already names X-ClickHouse-Summary.

${CLICKHOUSE_CURL} -sS -D - -o /dev/null "${CLICKHOUSE_URL}" --data-binary "SELECT 1" \
    | grep -i "^Access-Control-Expose-Headers:" \
    | tr -d ' ' | grep -c ",X-ClickHouse-Progress"
