#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest
# Tag no-fasttest: user manipulation is not supported there
# Tag zookeeper: `ON CLUSTER`

# `ALTER USER SELF AUTHENTICATION` is allowed only from a session whose authentication method lives at least as long
# as the account: a short-lived credential must not add a longer-lived one or remove one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="self_auth_session_deadline_${CLICKHOUSE_DATABASE}"

create_user()
{
    $CLICKHOUSE_CLIENT --query "
        DROP USER IF EXISTS $user;
        CREATE USER $user IDENTIFIED WITH $1;
        GRANT ALTER USER SELF AUTHENTICATION TO $user;
        GRANT CLUSTER ON *.* TO $user;"
}

as_user()
{
    local output
    if output=$($CLICKHOUSE_CLIENT --user "$user" --password "$1" --query "$2" 2>&1); then
        echo "ok"
    elif [[ "$output" == *"(ACCESS_DENIED)"* && "$output" == *"expires earlier than other authentication methods"* ]]; then
        echo "denied"
    else
        echo "$output"
    fi
}

methods()
{
    $CLICKHOUSE_CLIENT --query "SELECT arrayMap(x -> toString(x, 'UTC'), valid_until) FROM system.users WHERE name = '$user'"
}

echo "Token shorter than a permanent method"
create_user "sha256_password BY 'permanent', sha256_password BY 'token' VALID UNTIL '2099-01-01 00:00:00 UTC'"
as_user token "ALTER USER $user IDENTIFIED WITH sha256_password BY 'replaced'"
as_user token "ALTER USER $user ADD IDENTIFIED WITH sha256_password BY 'added'"
as_user token "ALTER USER $user ADD IDENTIFIED WITH sha256_password BY 'added' VALID UNTIL '2098-01-01 00:00:00 UTC'"
as_user token "ALTER USER $user RESET AUTHENTICATION METHODS TO NEW"
as_user token "ALTER USER $user ON CLUSTER test_shard_localhost IDENTIFIED WITH sha256_password BY 'replaced'"
methods
as_user permanent "ALTER USER $user ADD IDENTIFIED WITH sha256_password BY 'added'"
methods

echo "Only expiring methods"
create_user "sha256_password BY 'early' VALID UNTIL '2099-01-01 00:00:00 UTC', sha256_password BY 'late' VALID UNTIL '2100-01-01 00:00:00 UTC'"
as_user early "ALTER USER $user IDENTIFIED WITH sha256_password BY 'rotated'"
methods
as_user late "ALTER USER $user IDENTIFIED WITH sha256_password BY 'rotated'"
methods
as_user rotated "ALTER USER $user ADD IDENTIFIED WITH sha256_password BY 'added'"
methods

$CLICKHOUSE_CLIENT --query "DROP USER $user"
