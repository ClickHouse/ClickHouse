#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# After the output format fails to be created, the client cancels the query and must only drain the rest of the result,
# not build the output format again for every block. Every build starts the pager, so the pager must start once.
# With a large `interactive_delay` the server sends every block without reading the cancellation,
# and `sleepEachRow` gives every started pager the time to record its start before the next block arrives.

STARTS="${CLICKHOUSE_TMP:?}/pager_starts_${CLICKHOUSE_DATABASE:?}"
rm -f "${STARTS}"

${CLICKHOUSE_CLIENT} --pager "echo started >> '${STARTS}'; cat > /dev/null" --query "
    SELECT sleepEachRow(0.2) FROM numbers(3)
    SETTINGS max_block_size = 1, max_threads = 1, interactive_delay = 10000000
    FORMAT Template
" 2>&1 | grep -o 'INVALID_TEMPLATE_FORMAT' | sort -u

grep -c 'started' "${STARTS}"
rm -f "${STARTS}"
