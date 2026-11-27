#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Rules are global and their replacements run with the privileges of the user who activates them,
# so the rule privileges form their own `RULE ADMIN` group and are not part of the broad
# `CREATE` / `ALTER` / `DROP` groups: a user holding only those must not be able to manage rules.

RULE="rule_admin_${CLICKHOUSE_DATABASE}"
RULE_CREATED="rule_admin_created_${CLICKHOUSE_DATABASE}"
USER="user_rule_admin_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${RULE}"
$CLICKHOUSE_CLIENT --query "DROP RULE IF EXISTS ${RULE_CREATED}"
$CLICKHOUSE_CLIENT --query "CREATE USER ${USER} NOT IDENTIFIED"
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON system.query_rules TO ${USER}"
$CLICKHOUSE_CLIENT --query "CREATE RULE ${RULE} AS (SELECT '${RULE}') REWRITE TO (SELECT 2)"

run_as_user()
{
    $CLICKHOUSE_CLIENT --user "${USER}" --query "$1" 2>&1 | grep -o -m1 'ACCESS_DENIED' || echo "OK"
}

echo "with CREATE, ALTER, DROP ON *.*:"
$CLICKHOUSE_CLIENT --query "GRANT CREATE, ALTER, DROP ON *.* TO ${USER}"
run_as_user "CREATE RULE ${RULE_CREATED} AS (SELECT '${RULE_CREATED}') REWRITE TO (SELECT 3)"
run_as_user "ALTER RULE ${RULE} AS (SELECT '${RULE}') REWRITE TO (SELECT 4)"
run_as_user "DROP RULE ${RULE}"
$CLICKHOUSE_CLIENT --user "${USER}" --query "SELECT count() FROM system.query_rules WHERE name = '${RULE}'"

echo "with RULE ADMIN:"
$CLICKHOUSE_CLIENT --query "REVOKE CREATE, ALTER, DROP ON *.* FROM ${USER}"
$CLICKHOUSE_CLIENT --query "GRANT RULE ADMIN ON *.* TO ${USER}"
run_as_user "CREATE RULE ${RULE_CREATED} AS (SELECT '${RULE_CREATED}') REWRITE TO (SELECT 3)"
run_as_user "ALTER RULE ${RULE} AS (SELECT '${RULE}') REWRITE TO (SELECT 4)"
$CLICKHOUSE_CLIENT --user "${USER}" --query "SELECT count() FROM system.query_rules WHERE name = '${RULE}'"
run_as_user "DROP RULE ${RULE}"
run_as_user "DROP RULE ${RULE_CREATED}"

$CLICKHOUSE_CLIENT --query "DROP USER ${USER}"
