#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag justification:
#   no-fasttest: needs libpq, which is not built in fast test.
#   no-parallel: the database created here points at an unreachable host and is visible in
#     system.tables, so a concurrent scan of it without a database filter would connect there too.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dictionary name that points into a remote database must not make the server query the remote side
# before the access check: a user without access gets `ACCESS_DENIED`, not an error from the remote server.

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
remote_db="${CLICKHOUSE_DATABASE}_postgresql"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    DROP DATABASE IF EXISTS ${remote_db};
    CREATE DATABASE ${remote_db} ENGINE = PostgreSQL('127.0.0.1:1', 'db', 'user', 'password');
    CREATE USER ${username} NOT IDENTIFIED;
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${username};
"

function as_user()
{
    echo "$1" | sed "s/${remote_db}/remote_db/g"
    local output
    output=$(${CLICKHOUSE_CLIENT} --user "${username}" --query "$1" 2>&1)
    grep -o -m1 -E "ACCESS_DENIED|POSTGRESQL_CONNECTION_FAILURE|Connection refused" <<< "${output}" | head -1
}

as_user "SELECT dictHas('${remote_db}.t', toUInt64(1))"
as_user "SELECT dictGet('${remote_db}.t', 'value', toUInt64(1))"
as_user "SELECT dictGet(${remote_db}.t, 'value', toUInt64(1))"
as_user "SELECT naiveBayesClassifier('${remote_db}.t', 'good')"
as_user "SELECT * FROM dictionary('${remote_db}.t')"

${CLICKHOUSE_CLIENT} -m --query "
    DROP DATABASE ${remote_db};
    DROP USER ${username};
"
