#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A statement of the `q` dialect continues over blank and comment-only lines to an indented line.
# The lexer looks ahead over such lines after a line break, and it has to do that once for a run of them:
# with a look ahead from every line break, these queries of about 100 KB would take billions of iterations.

# The command substitution drops the trailing line breaks, hence the `x` at the end.
blank_lines=$(printf '\n%.0s' $(seq 1 100000); echo x)
blank_lines=${blank_lines%x}
comment_lines=$(printf '/ a comment\n%.0s' $(seq 1 8000); echo x)
comment_lines=${comment_lines%x}

function q()
{
    $CLICKHOUSE_CLIENT --enable_q_dialect 1 --dialect q
}

echo "--- blank lines"
printf '%s' "1+
${blank_lines} 2" | q

echo "--- comment-only lines"
printf '%s' "1+
${comment_lines} 2" | q

echo "--- blank lines before a line in the first column: two statements"
printf '%s' "a: 5
${blank_lines}a * 2" | q
