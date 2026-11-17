#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: the access storage is replicated there

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Under readonly, granting a role is refused like granting a privilege, and revoking one is refused ON CLUSTER.

A="a_${CLICKHOUSE_TEST_UNIQUE_NAME}"
B="b_${CLICKHOUSE_TEST_UNIQUE_NAME}"
R="r_${CLICKHOUSE_TEST_UNIQUE_NAME}"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $A, $B"
    $CLICKHOUSE_CLIENT -q "DROP ROLE IF EXISTS $R"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -m -q "
CREATE USER $A IDENTIFIED WITH no_password;
CREATE USER $B IDENTIFIED WITH no_password;
CREATE ROLE $R;
GRANT $R TO $A WITH ADMIN OPTION;
GRANT SELECT ON system.one TO $A WITH GRANT OPTION;
GRANT CLUSTER ON *.* TO $A;
"

# Prints the exception name, e.g. (READONLY), or OK.
run() { $CLICKHOUSE_CLIENT --user "$A" --readonly "$1" -q "$2" 2>&1 >/dev/null | grep -o -m1 -E '\([A-Z_]+\)$' || echo OK; }
granted() { $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.role_grants WHERE user_name = '$B' AND granted_role_name = '$R'"; }

run 1 "GRANT SELECT ON system.one TO $B"
run 1 "GRANT $R TO $B"
run 2 "GRANT $R TO $B"
run 1 "GRANT $R TO $B ON CLUSTER test_shard_localhost"
granted

$CLICKHOUSE_CLIENT -q "GRANT $R TO $B"
run 1 "REVOKE $R FROM $B ON CLUSTER test_shard_localhost"
granted
run 1 "REVOKE $R FROM $B"
granted

run 0 "GRANT $R TO $B"
granted
