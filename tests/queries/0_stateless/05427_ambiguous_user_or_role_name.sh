#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A name that is both a user and a role is refused in lists that accept either
# (the stateless config enables `access_control_improvements.throw_on_ambiguous_user_or_role_names`).

name="ambiguous_${CLICKHOUSE_DATABASE}"
granted="granted_${CLICKHOUSE_DATABASE}"
other="other_${CLICKHOUSE_DATABASE}"
new="new_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "
        DROP ROW POLICY IF EXISTS p_$name ON t;
        DROP QUOTA IF EXISTS q_$name;
        DROP USER IF EXISTS $name, $other, $new, $granted;
        DROP ROLE IF EXISTS $name, $granted;"
}

function check_refused()
{
    if $CLICKHOUSE_CLIENT --query "$2" 2>&1 | grep -q 'is ambiguous'; then
        echo "$1: refused"
    else
        echo "$1: NOT refused"
    fi
}

cleanup

# The user gets its grant before the same-named role exists.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t (x UInt8) ENGINE = Memory;
    CREATE USER $name IDENTIFIED WITH no_password;
    CREATE USER $other IDENTIFIED WITH no_password;
    CREATE ROLE $granted;
    GRANT SELECT ON t TO $name WITH GRANT OPTION;
    CREATE ROLE $name;"

echo "--- CURRENT_USER is not a name"
$CLICKHOUSE_CLIENT --user "$name" --query "GRANT SELECT ON t TO CURRENT_USER" && echo "OK"

echo "--- refused"
check_refused "GRANT role TO" "GRANT $granted TO $name"
check_refused "GRANT privilege TO" "GRANT INSERT ON t TO $name"
check_refused "REVOKE FROM" "REVOKE SELECT ON t FROM $name"
check_refused "CREATE ROW POLICY TO" "CREATE ROW POLICY p_$name ON t USING x = 1 TO $name"
check_refused "CREATE QUOTA TO ALL EXCEPT" "CREATE QUOTA q_$name TO ALL EXCEPT $name"
check_refused "CREATE USER GRANTEES" "CREATE USER $new IDENTIFIED WITH no_password GRANTEES $name"
check_refused "CREATE USER GRANTEES itself" "CREATE USER $granted IDENTIFIED WITH no_password GRANTEES $granted"

echo "--- nothing changed"
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM system.role_grants WHERE granted_role_name = '$granted';
    SELECT arraySort(groupArray(toString(access_type))) FROM system.grants WHERE user_name = '$name';
    SELECT count() FROM system.grants WHERE role_name = '$name';
    SELECT count() FROM system.row_policies WHERE short_name = 'p_$name';
    SELECT count() FROM system.quotas WHERE name = 'q_$name';
    SELECT count() FROM system.users WHERE name IN ('$new', '$granted');"

echo "--- a role-only list is not ambiguous"
$CLICKHOUSE_CLIENT --query "
    GRANT $name TO $other;
    SELECT count() FROM system.role_grants WHERE user_name = '$other' AND granted_role_name = '$name';"

echo "--- without the user, the name is the role"
$CLICKHOUSE_CLIENT --query "
    DROP USER $name;
    GRANT $granted TO $name;
    SELECT count() FROM system.role_grants WHERE role_name = '$name' AND granted_role_name = '$granted';"

cleanup
