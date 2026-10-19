#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The `TOO_DEEP_RECURSION` message must not contain the stack or frame address. Sent over HTTP, so that
# the server parses the query, not the client.
response=$(python3 -c "print('SELECT ' + '(' * 100000 + '1' + ')' * 100000)" \
    | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_parser_depth=0&max_parser_backtracks=0&max_query_size=10000000" --data-binary @-)

if [[ "$response" == *TOO_DEEP_RECURSION* && "$response" != *0x* ]]; then
    echo "OK"
else
    echo "$response"
fi
