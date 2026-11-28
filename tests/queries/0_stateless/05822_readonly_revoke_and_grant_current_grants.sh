#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: the access storage is replicated there

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Under readonly, revoking a privilege and GRANT CURRENT GRANTS are refused like granting a privilege.

A="a_${CLICKHOUSE_TEST_UNIQUE_NAME}"
B="b_${CLICKHOUSE_TEST_UNIQUE_NAME}"
C="c_${CLICKHOUSE_TEST_UNIQUE_NAME}"

cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $A, $B, $C"
}
trap cleanup EXIT
cleanup

# A's grant option covers everything B holds, but not the element `SELECT ON *.*` itself.
$CLICKHOUSE_CLIENT -m -q "
CREATE USER $A IDENTIFIED WITH no_password;
CREATE USER $B IDENTIFIED WITH no_password;
GRANT SELECT ON *.* TO $A WITH GRANT OPTION;
REVOKE SELECT ON system.* FROM $A;
GRANT SELECT ON *.* TO $B WITH GRANT OPTION;
REVOKE SELECT ON system.* FROM $B;
"

# Prints the exception name, e.g. (READONLY), or OK.
run() { $CLICKHOUSE_CLIENT --user "$A" --readonly "$1" -q "$2" 2>&1 >/dev/null | grep -o -m1 -E '\([A-Z_]+\)$' || echo OK; }
# The same over HTTP GET, which implies readonly = 2.
get() { ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=$A" --get --data-urlencode "query=$1" | grep -o -m1 -E '\([A-Z_]+\)' || echo OK; }
grants() { $CLICKHOUSE_CLIENT -q "SELECT count(), countIf(grant_option), countIf(is_partial_revoke) FROM system.grants WHERE user_name = '$B'"; }

run 1 "REVOKE GRANT OPTION FOR SELECT ON *.* FROM $B"
run 1 "REVOKE SELECT ON *.* FROM $B"
run 2 "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.* FROM $B"
run 1 "REVOKE ALL ON *.* FROM $B"
get "REVOKE SELECT ON *.* FROM $B"
grants

# B holds nothing in `system`, so this revoke changes nothing.
run 1 "REVOKE SELECT ON system.numbers FROM $B"
run 0 "REVOKE SELECT ON *.* FROM $B"
grants

run 1 "GRANT CURRENT GRANTS(SELECT ON *.*) TO $B"
run 2 "GRANT CURRENT GRANTS(SELECT ON *.*) TO $B"
run 1 "GRANT CURRENT GRANTS(SELECT ON *.*) TO $B WITH REPLACE OPTION"
get "GRANT CURRENT GRANTS(SELECT ON *.*) TO $B"
grants
run 0 "GRANT CURRENT GRANTS(SELECT ON *.*) TO $B"
grants

# ON CLUSTER: the initiator's check passes the temporary database without reaching readonly.
$CLICKHOUSE_CLIENT -m -q "
GRANT CLUSTER ON *.* TO $A;
CREATE USER $C IDENTIFIED WITH no_password;
GRANT SELECT ON _temporary_and_external_tables.* TO $C;
"
run 1 "REVOKE SELECT ON _temporary_and_external_tables.* FROM $C ON CLUSTER test_shard_localhost"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.grants WHERE user_name = '$C' AND database = '_temporary_and_external_tables'"
