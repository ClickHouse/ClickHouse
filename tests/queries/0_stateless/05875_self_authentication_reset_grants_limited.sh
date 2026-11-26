#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: user manipulation is not supported there

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="self_auth_reset_${CLICKHOUSE_DATABASE}"
password="SamePassword_05875"
table="${CLICKHOUSE_DATABASE}.t_05875"

# Both methods accept the same password, so a login is limited by the `GRANTS` clause of the first one.
# Resetting to the newest (unrestricted) method from such a session would lift that limit.
$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS $user;
    CREATE TABLE $table (x UInt8) ENGINE = Memory;
    CREATE USER $user IDENTIFIED WITH
        sha256_password BY '$password' GRANTS (ALTER USER SELF AUTHENTICATION),
        sha256_password BY '$password';
    GRANT ALTER USER SELF AUTHENTICATION TO $user;
    GRANT SELECT ON $table TO $user;"

$CLICKHOUSE_CLIENT --user "$user" --password "$password" --query "ALTER USER $user RESET AUTHENTICATION METHODS TO NEW" 2>&1 \
    | grep -o "such sessions cannot change authentication methods" | head -1
$CLICKHOUSE_CLIENT --query "SELECT length(auth_type) FROM system.users WHERE name = '$user'"
$CLICKHOUSE_CLIENT --user "$user" --password "$password" --query "SELECT x FROM $table" 2>&1 | grep -o "ACCESS_DENIED" | head -1

$CLICKHOUSE_CLIENT --query "DROP USER $user"
