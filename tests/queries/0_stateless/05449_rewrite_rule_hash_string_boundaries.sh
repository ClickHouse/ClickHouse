#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The rewrite-rule matcher treats an equal tree hash as semantic equality. The tree hashes of
# several statements folded adjacent strings without their lengths, so statements whose strings
# differ only in where one ends and the next begins hashed the same, and a rule for one of them
# also rewrote or rejected the other. Only whether the rule fired is asserted, so the statements
# themselves need not succeed.
# Rule names are global; make them unique per test database so the test is parallel-safe.

RULE="rule_boundaries_${CLICKHOUSE_DATABASE}"

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

pair "CREATE SETTINGS PROFILE ab, c" "CREATE SETTINGS PROFILE a, bc"
pair "CREATE NAMED COLLECTION n AS x = 12" "CREATE NAMED COLLECTION n AS x1 = 2"
pair "DROP ROW POLICY p ON a.bc" "DROP ROW POLICY p ON ab.c"
pair "CREATE QUOTA ab, c" "CREATE QUOTA a, bc"
