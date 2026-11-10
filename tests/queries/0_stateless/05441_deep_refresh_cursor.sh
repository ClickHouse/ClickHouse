#!/usr/bin/env bash
# Tags: no-fasttest
# A cursor with an absurdly deep structure in `STREAM CURSOR` of a query must be rejected with an exception instead of exhausting the stack.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DEEP_PATH="a$(printf '.a%.0s' {1..100000})"
echo "SELECT 1 FROM system.one STREAM CURSOR {'${DEEP_PATH}': 1}" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&max_query_size=10000000" --data-binary @- | grep -o -m1 'BAD_ARGUMENTS'
