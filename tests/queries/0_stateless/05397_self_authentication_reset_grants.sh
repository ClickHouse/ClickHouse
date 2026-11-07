#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# User management is unavailable in fast tests; database grants differ across replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

user="self_auth_reset_${CLICKHOUSE_DATABASE}_${RANDOM}"
table="${CLICKHOUSE_DATABASE}.self_auth_reset_${RANDOM}"
password="SamePassword_05397"

admin()
{
    $CLICKHOUSE_CLIENT --multiquery --query "$1"
}

login()
{
    $CLICKHOUSE_CLIENT --user "$user" --password "$password" --query "$1"
}

cleanup()
{
    admin "DROP USER IF EXISTS $user; DROP TABLE IF EXISTS $table;" >/dev/null
}
trap cleanup EXIT

expect_denied()
{
    local output
    if output=$(login "$1" 2>&1); then
        echo "Expected ACCESS_DENIED: $1" >&2
        exit 1
    fi
    if [[ "$output" != *"(ACCESS_DENIED)"* || "$output" != *"$2"* ]]; then
        echo "Unexpected error: $output" >&2
        exit 1
    fi
}

# Both methods accept the same password. Login intersects their grant limits;
# resetting to the unrestricted last method would remove the SELECT restriction.
admin "CREATE TABLE $table (value UInt8) ENGINE = Memory;
       INSERT INTO $table VALUES (42);
       CREATE USER $user IDENTIFIED WITH
           sha256_password BY '$password' GRANTS (ALTER USER SELF AUTHENTICATION),
           sha256_password BY '$password';
       GRANT ALTER USER SELF AUTHENTICATION TO $user;
       GRANT SELECT ON $table TO $user;"

before=$(admin "SHOW CREATE USER $user")
[[ $(admin "SELECT length(auth_type) FROM system.users WHERE name = '$user'") == 2 ]]
expect_denied "SELECT value FROM $table" "SELECT"
expect_denied "ALTER USER $user RESET AUTHENTICATION METHODS TO NEW" \
    "such sessions cannot change authentication methods"

[[ $(admin "SHOW CREATE USER $user") == "$before" ]]
[[ $(admin "SELECT length(auth_type) FROM system.users WHERE name = '$user'") == 2 ]]
# Each call starts a fresh authenticated connection.
expect_denied "SELECT value FROM $table" "SELECT"
echo "Grant-limited reset denied; both methods and login restrictions preserved"
