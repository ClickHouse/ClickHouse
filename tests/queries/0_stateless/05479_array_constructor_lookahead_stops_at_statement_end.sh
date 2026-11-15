#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

run()
{
    echo "--- $1"
    ${CLICKHOUSE_LOCAL} --query "$1" 2>&1 \
        | sed -e 's/Expected one of: .*//' -e 's/Expected .*\.$//' -e 's/ (version [^)]*)//' -e 's/^Code: 62. DB::Exception: //' \
        | sed -e 's/[[:space:]]*$//'
}

# `ARRAY[` is not closed before the `;`, and the next statement has a closing bracket or a comma:
# the error must point at the end of the first statement.
run "SELECT ARRAY[f(1); 2]"
run "SELECT ARRAY[f(1); SELECT 1, 2]"
