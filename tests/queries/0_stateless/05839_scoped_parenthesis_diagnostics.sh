#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A closing parenthesis in a later statement must not balance the first statement.
printf '%s\n' 'SELECT (1; SELECT 2);' |
    ${CLICKHOUSE_FORMAT} --multiquery 2>&1 | grep -cF 'Unmatched parentheses'

# A semicolon inside a quoted string or a comment is not a statement delimiter.
printf '%s\n' "SELECT (';'); /* ; ) */ SELECT (2);" |
    ${CLICKHOUSE_LOCAL}

# A syntax error in the second statement must still be reported.
printf '%s\n' 'SELECT 1; SELECT 2);' |
    ${CLICKHOUSE_FORMAT} --multiquery 2>&1 | grep -cF 'Unmatched parentheses'
