#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest
# Tag no-fasttest: user manipulation is not supported there

# Authentication methods added with `ALTER USER SELF AUTHENTICATION` must not outlive the account,
# also when the query is distributed `ON CLUSTER` and replicas execute it without a user.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="self_auth_valid_until_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS $user;
    CREATE USER $user IDENTIFIED WITH sha256_password BY 'p1' VALID UNTIL '2100-01-01 00:00:00 UTC';
    GRANT ALTER USER SELF AUTHENTICATION TO $user;
    GRANT CLUSTER ON *.* TO $user;"

as_user()
{
    $CLICKHOUSE_CLIENT --user "$user" --password "$1" --distributed_ddl_output_mode none --query "$2"
    $CLICKHOUSE_CLIENT --query "SELECT arrayMap(x -> toString(x, 'UTC'), valid_until) FROM system.users WHERE name = '$user'"
}

as_user p1 "ALTER USER $user IDENTIFIED WITH sha256_password BY 'p2'"
as_user p2 "ALTER USER $user ADD IDENTIFIED WITH sha256_password BY 'p3'"
as_user p2 "ALTER USER $user ADD IDENTIFIED WITH sha256_password BY 'p4' VALID UNTIL '2200-01-01 00:00:00 UTC'"
as_user p2 "ALTER USER $user ADD IDENTIFIED WITH sha256_password BY 'p5' VALID UNTIL '2099-01-01 00:00:00 UTC'"
as_user p2 "ALTER USER $user ON CLUSTER test_shard_localhost IDENTIFIED WITH sha256_password BY 'p6'"

$CLICKHOUSE_CLIENT --query "DROP USER $user"
