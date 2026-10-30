#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas, no-ordinary-database
# A query plan cache hit checks access before the interpreter is returned, so a hit denied after
# the grant was revoked fails inside the plan cache path. It must still be counted in `Query`,
# `SelectQuery`, `InitialQuery` and `InitialSelectQuery`, like the same failing query without the cache.
# The plan cache is server-wide, and the test creates a global user, so it runs in isolation (see 04489).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05460_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"
QUERY="SELECT sum(id) FROM ${CLICKHOUSE_DATABASE}.t_05460"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_05460;
    CREATE TABLE t_05460 (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_05460 VALUES (1), (2);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05460 TO $user;
    SYSTEM DROP QUERY PLAN CACHE;
"

# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT --user="$user" $SETTINGS --log_comment=05460_warm --query "$QUERY"
$CLICKHOUSE_CLIENT --query "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.t_05460 FROM $user"
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT --user="$user" $SETTINGS --log_comment=05460_denied_on --query "$QUERY" 2>&1 | grep -o -m1 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT --user="$user" --log_comment=05460_denied_off --query "$QUERY" 2>&1 | grep -o -m1 'ACCESS_DENIED'

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT log_comment, type, ProfileEvents['Query'], ProfileEvents['SelectQuery'], ProfileEvents['InitialQuery'], ProfileEvents['InitialSelectQuery']
    FROM system.query_log
    WHERE event_date >= yesterday() AND type != 'QueryStart' AND log_comment IN ('05460_warm', '05460_denied_on', '05460_denied_off') AND user = '$user'
    ORDER BY event_time_microseconds"

$CLICKHOUSE_CLIENT --query "
    DROP USER $user;
    DROP TABLE t_05460;
"
