#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The tree hashes of `CREATE` / `ALTER NAMED COLLECTION` fold their AST-valued values (kept inside a
# `Field`, outside `children`), and the tree hash of a CTE folds its column alias list (also outside
# `children`), so the AST limits checked before the rewrite-rule matcher must count them too: both in
# a rule template and in a submitted query while rules are active.
# Rule names are global; make them unique per test database so the test is parallel-safe.

ACTIVE="rule_active_${CLICKHOUSE_DATABASE}"
TEMPLATE="rule_template_${CLICKHOUSE_DATABASE}"
DISK_VALUE="disk(path = concat('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'))"
CTE="WITH t (c_a, c_b, c_c, c_d, c_e, c_f, c_g, c_h, c_i, c_j) AS (SELECT 1, 2, 3, 4, 5, 6, 7, 8, 9, 10) SELECT 1"

$CLICKHOUSE_CLIENT --query "CREATE RULE ${ACTIVE} AS (SELECT 'nomatch') REWRITE TO (SELECT 1)"

check() # $1 label, $2 max_ast_elements, $3 query
{
    out=$($CLICKHOUSE_CLIENT --query_rules "${ACTIVE}" --max_ast_elements "$2" --query "$3" 2>&1 || true)
    if echo "$out" | grep -q "TOO_BIG_AST"; then echo "$1: too big"; else echo "$1: accepted"; fi
}

check "template CREATE NAMED COLLECTION, small limit" 10 "CREATE RULE ${TEMPLATE} AS (CREATE NAMED COLLECTION c AS storage = ${DISK_VALUE}) REJECT WITH 'blocked'"
check "template CREATE NAMED COLLECTION, generous limit" 1000 "CREATE RULE ${TEMPLATE} AS (CREATE NAMED COLLECTION c AS storage = ${DISK_VALUE}) REJECT WITH 'blocked'"
$CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${TEMPLATE}"
check "template ALTER NAMED COLLECTION, small limit" 10 "CREATE RULE ${TEMPLATE} AS (ALTER NAMED COLLECTION c SET storage = ${DISK_VALUE}) REJECT WITH 'blocked'"
$CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${TEMPLATE}"

check "submitted CREATE NAMED COLLECTION, small limit" 10 "CREATE NAMED COLLECTION c_${CLICKHOUSE_DATABASE} AS storage = ${DISK_VALUE}"

check "template CTE aliases, small limit" 30 "CREATE RULE ${TEMPLATE} AS (${CTE}) REJECT WITH 'blocked'"
check "template CTE aliases, generous limit" 1000 "CREATE RULE ${TEMPLATE} AS (${CTE}) REJECT WITH 'blocked'"
$CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${TEMPLATE}"

check "submitted CTE aliases, small limit" 30 "${CTE}"
check "submitted CTE aliases, generous limit" 1000 "${CTE}"

$CLICKHOUSE_CLIENT --query "DROP RULE ${ACTIVE}"
