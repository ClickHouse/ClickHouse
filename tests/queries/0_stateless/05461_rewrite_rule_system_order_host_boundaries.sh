#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A rewrite rule must match its own source statement and nothing else.
# - `SYSTEM SCHEDULE MERGE t PARTS ...`: the parser put the parts list before the table in `children`,
#   while `ASTSystemQuery::clone` (used for the rule source template) puts the table first, so the
#   rule never matched its own source statement.
# - `CREATE USER ... HOST NAME`: the host names were folded into the tree hash without their lengths,
#   so `HOST NAME 'ab', 'c'` hashed the same as `HOST NAME 'a', 'bc'` and a rule for one matched both.
# Only whether the rule fired is asserted, so the statements themselves need not succeed.
# Rule names are global; make them unique per test database so the test is parallel-safe.

RULE="rule_order_host_${CLICKHOUSE_DATABASE}"

check() # $1 label, $2 query
{
    out=$($CLICKHOUSE_CLIENT --query_rules "${RULE}" --query "$2" 2>&1 || true)
    if echo "$out" | grep -q "REWRITE_RULE_REJECTION"; then echo "$1: rejected"; else echo "$1: not rejected"; fi
}

pair() # $1 template, $2 other statement
{
    $CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS ($1) REJECT WITH 'blocked'"
    check "$1" "$1"
    check "$2" "$2"
    $CLICKHOUSE_CLIENT --query "DROP RULE ${RULE}"
}

pair "SYSTEM SCHEDULE MERGE t PARTS 'p1', 'p2'" "SYSTEM SCHEDULE MERGE t PARTS 'p1', 'p3'"
pair "CREATE USER u HOST NAME 'ab', 'c'" "CREATE USER u HOST NAME 'a', 'bc'"
pair "CREATE USER u HOST REGEXP 'ab', 'c'" "CREATE USER u HOST REGEXP 'a', 'bc'"
pair "CREATE USER u HOST LIKE 'ab', 'c'" "CREATE USER u HOST LIKE 'a', 'bc'"
