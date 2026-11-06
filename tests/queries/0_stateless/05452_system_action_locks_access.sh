#!/usr/bin/env bash

# Checks the local privilege checks of `SYSTEM STOP/START VIRTUAL PARTS UPDATE` and `SYSTEM STOP/START REDUCE BLOCKING PARTS`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05452_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user};
    CREATE TABLE target (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE other (x UInt64) ENGINE = MergeTree ORDER BY x;
"

function run_as_user()
{
    ${CLICKHOUSE_CLIENT} --user "${user}" --query "$1" 2>&1 | grep -o -m1 'ACCESS_DENIED' || echo 'OK'
}

for action in 'VIRTUAL PARTS UPDATE' 'REDUCE BLOCKING PARTS'
do
    echo "--- ${action}"

    echo "no grant"
    run_as_user "SYSTEM STOP ${action} ${CLICKHOUSE_DATABASE}.target"
    run_as_user "SYSTEM START ${action} ${CLICKHOUSE_DATABASE}.target"

    echo "grant on another table"
    ${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM STOP ${action} ON ${CLICKHOUSE_DATABASE}.other TO ${user}"
    run_as_user "SYSTEM STOP ${action} ${CLICKHOUSE_DATABASE}.target"
    run_as_user "SYSTEM START ${action} ${CLICKHOUSE_DATABASE}.target"

    echo "grant on the target table"
    ${CLICKHOUSE_CLIENT} --query "GRANT SYSTEM STOP ${action} ON ${CLICKHOUSE_DATABASE}.target TO ${user}"
    run_as_user "SYSTEM STOP ${action} ${CLICKHOUSE_DATABASE}.target"
    run_as_user "SYSTEM START ${action} ${CLICKHOUSE_DATABASE}.target"

    ${CLICKHOUSE_CLIENT} --query "REVOKE ALL ON *.* FROM ${user}"
done

${CLICKHOUSE_CLIENT} --query "DROP USER ${user}"
