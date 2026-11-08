#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `rows_before_limit_at_least` is reported in the `X-ClickHouse-Summary` HTTP header, with the same value as in the
# `JSON` output formats, so that clients using other formats (like `RowBinary`) can get it too.
# `exact_rows_before_limit` makes the values deterministic.
SETTINGS="http_wait_end_of_query=1&exact_rows_before_limit=1"

# Prints the value of a field of the `X-ClickHouse-Summary` header, or `absent`.
function summary_field()
{
    local field="$1"
    local query="$2"
    local value
    value=$(${CLICKHOUSE_CURL} -sS -D - -o /dev/null "${CLICKHOUSE_URL}&${SETTINGS}&default_format=RowBinary" --data-binary "${query}" \
        | tr -d '\r' | grep -i '^X-ClickHouse-Summary:' | grep -o "\"${field}\":\"[0-9]*\"" | grep -o '[0-9]\+')
    echo "${value:-absent}"
}

# Prints `rows_before_limit_at_least` from the `JSON` output format, or `absent`.
function json_rows_before_limit()
{
    local query="$1"
    local value
    value=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&${SETTINGS}&default_format=JSON" --data-binary "${query}" \
        | grep -o '"rows_before_limit_at_least": [0-9]*' | grep -o '[0-9]\+$')
    echo "${value:-absent}"
}

function check()
{
    local query="$1"
    echo "${query}"
    echo "summary: $(summary_field rows_before_limit_at_least "${query}"), JSON: $(json_rows_before_limit "${query}")"
}

check "SELECT number FROM numbers(100) WHERE number % 2 = 0 LIMIT 10"
check "SELECT number FROM numbers(100) WHERE number % 2 = 0 LIMIT 10 SETTINGS max_block_size = 7"
check "SELECT number FROM numbers(100) WHERE number % 2 = 0 LIMIT 10 OFFSET 5"
check "SELECT number FROM numbers(1000) ORDER BY number DESC LIMIT 5"
check "SELECT number % 7 AS k, count() FROM numbers(1000) GROUP BY k ORDER BY k LIMIT 3"
check "SELECT number FROM remote('127.0.0.{1,2}', numbers(100)) WHERE number % 2 = 0 LIMIT 10"
# A limit that leaves no rows still reports the counter.
check "SELECT number FROM numbers(100) WHERE number > 1000 LIMIT 10"
# Without a limit the field is absent, as in the `JSON` formats.
check "SELECT number FROM numbers(100) WHERE number % 2 = 0"

echo "result_rows of a query with a limit"
summary_field result_rows "SELECT number FROM numbers(100) WHERE number % 2 = 0 LIMIT 10"

# The field is a final value, so it is in the summary but not in the intermediate `X-ClickHouse-Progress` headers.
PROGRESS_SETTINGS="${SETTINGS}&default_format=RowBinary&send_progress_in_http_headers=1&http_headers_progress_interval_ms=0&max_block_size=10"
HEADERS=$(${CLICKHOUSE_CURL} -sS -D - -o /dev/null "${CLICKHOUSE_URL}&${PROGRESS_SETTINGS}" \
    --data-binary "SELECT number FROM numbers(100) WHERE number % 2 = 0 LIMIT 40" | tr -d '\r')
[ "$(echo "${HEADERS}" | grep -c -i '^X-ClickHouse-Progress:')" -gt 0 ] && echo "Has X-ClickHouse-Progress headers"
echo "X-ClickHouse-Progress headers with rows_before_limit_at_least: $(echo "${HEADERS}" | grep -i '^X-ClickHouse-Progress:' | grep -c 'rows_before_limit_at_least')"
echo "X-ClickHouse-Summary: $(echo "${HEADERS}" | grep -i '^X-ClickHouse-Summary:' | grep -o '"rows_before_limit_at_least":"[0-9]*"')"
