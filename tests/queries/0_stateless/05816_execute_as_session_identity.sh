#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the interserver secret requires SSL

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# After `EXECUTE AS target` switched a session, a reattached named session, a distributed query
# authenticated with the interserver secret, and a background query must all run as `target`,
# not as the user who logged in.

login="login_${CLICKHOUSE_DATABASE}"
target="target_${CLICKHOUSE_DATABASE}"
session="session_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${login}, ${target};
    CREATE USER ${login}, ${target};
    CREATE TABLE t_login (n UInt64) ENGINE = MergeTree ORDER BY n;
    CREATE TABLE t_target (n UInt64) ENGINE = MergeTree ORDER BY n;
    INSERT INTO t_target VALUES (1);
    GRANT IMPERSONATE ON ${target} TO ${login};
    GRANT SELECT ON t_login TO ${login};
    GRANT SELECT, INSERT ON t_target TO ${target};
    GRANT REMOTE ON *.* TO ${target};"

function http()
{
    local params=$1 query=$2
    $CLICKHOUSE_CURL -sS "${CLICKHOUSE_URL}&user=${login}&session_id=${session}${params}" -d "$query"
}

http "" "EXECUTE AS ${target}"

echo '--- a reattached named session'
http "" "SELECT currentUser() = '${target}', authenticatedUser() = '${login}'"
http "" "SELECT count() FROM t_login" | grep -o -m1 ACCESS_DENIED

echo '--- a distributed query with the interserver secret'
http "" "SELECT count() FROM cluster('test_cluster_interserver_secret', currentDatabase(), t_target) SETTINGS prefer_localhost_replica = 0"

echo '--- background queries'
http "&run_query_in_background=1" "SELECT count() FROM t_login" | grep -o -m1 ACCESS_DENIED
http "&run_query_in_background=1" "INSERT INTO t_target SELECT 2"
$CLICKHOUSE_CLIENT --user "${login}" -q "
    EXECUTE AS ${target};
    SELECT count() FROM t_login SETTINGS run_query_in_background = 1;" 2>&1 | grep -o -m1 ACCESS_DENIED
$CLICKHOUSE_CLIENT --user "${login}" -q "
    EXECUTE AS ${target};
    INSERT INTO t_target SETTINGS run_query_in_background = 1 SELECT 3;"

start=$EPOCHSECONDS
while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM t_target WHERE n IN (2, 3)") != 2 ]]; do
    if ((EPOCHSECONDS - start > 180)); then
        echo "Timeout waiting for the background inserts" >&2
        exit 1
    fi
    sleep 0.3
done
echo "inserted by background queries"

$CLICKHOUSE_CLIENT -q "DROP USER ${login}, ${target}"
