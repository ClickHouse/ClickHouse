#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Test hint comments are documented in
# docs/resources/develop-contribute/contribute/tests.mdx ("Test Hints"): a query text may
# carry `-- { serverError <code|NAME> }`, `-- { clientError ... }` or
# `-- { retry <N> until|while ... }`. The client parses those hints itself
# (src/Client/TestHint.cpp) before it sends anything, so a malformed hint must be rejected
# by the client with the error code CANNOT_PARSE_TEXT and a non-zero exit status.
#
# Each case below prints: the case name, the error code name the client reports, and the
# client exit status.

hint() {
    local out
    local rc
    out=$($CLICKHOUSE_CLIENT --query "SELECT 1; -- { $2 }" 2>&1 >/dev/null)
    rc=$?
    printf '%s\t%s\t%s\n' "$1" "$(printf '%s\n' "$out" | grep -oE '\([A-Z_]+\)' | head -1 | tr -d '()')" "$rc"
}

# The number of retries is not a number.
hint 'retries-not-a-number' 'retry foo while'
# Neither `until` nor `while` follows the number of retries.
hint 'retries-without-until-while' 'retry 2 forever'
# `retry` needs exactly one expected server error.
hint 'retry-without-one-server-error' 'retry 2 while'
# An error code that is a number token but not an integer.
hint 'error-code-not-an-integer' 'serverError 1, .5 '
# An error code that is neither a number nor a bare word.
hint 'error-code-wrong-token' 'serverError .5'
# Two error codes without the separating comma.
hint 'error-codes-without-comma' 'serverError 1 2'

# A well formed hint is accepted: the client recognises the expected server error, prints
# nothing and exits with 0.
good=$($CLICKHOUSE_CLIENT --query "SELECT throwIf(1, 'malformed hint probe'); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }" 2>&1)
good_rc=$?
printf 'well-formed-hint\t%s\t%s\n' "${good:-ok}" "$good_rc"
