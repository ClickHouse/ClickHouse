#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The tree hash of `SET ROLE` folds its role list, which is kept outside `children`, so the AST
# limits checked before the rewrite-rule matcher must count the role names too: both in a rule
# template and in a submitted query while rules are active.
# Rule names are global; make them unique per test database so the test is parallel-safe.

ACTIVE="rule_active_${CLICKHOUSE_DATABASE}"
ROLES="rule_roles_${CLICKHOUSE_DATABASE}"
ROLE_LIST="r_a, r_b, r_c, r_d, r_e, r_f, r_g, r_h, r_i, r_j"

$CLICKHOUSE_CLIENT --query "CREATE RULE ${ACTIVE} AS (SELECT 'nomatch') REWRITE TO (SELECT 1)"

check() # $1 label, $2 max_ast_elements, $3 query
{
    out=$($CLICKHOUSE_CLIENT --query_rules "${ACTIVE}" --max_ast_elements "$2" --query "$3" 2>&1 || true)
    if echo "$out" | grep -q "TOO_BIG_AST"; then echo "$1: too big"; else echo "$1: accepted"; fi
}

check "template, small limit" 8 "CREATE RULE ${ROLES} AS (SET ROLE ${ROLE_LIST}) REJECT WITH 'blocked'"
check "template, generous limit" 1000 "CREATE RULE ${ROLES} AS (SET ROLE ${ROLE_LIST}) REJECT WITH 'blocked'"
$CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${ROLES}"

check "submitted SET ROLE, small limit" 8 "SET ROLE ${ROLE_LIST}"
check "submitted SET ROLE, generous limit" 1000 "SET ROLE ${ROLE_LIST}"
check "submitted SET DEFAULT ROLE, small limit" 8 "SET DEFAULT ROLE ${ROLE_LIST} TO u_a, u_b"
check "submitted SHOW GRANTS, small limit" 8 "SHOW GRANTS FOR ${ROLE_LIST}"

$CLICKHOUSE_CLIENT --query "DROP RULE ${ACTIVE}"
