#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test checks the `DETACH`/`ATTACH` the hook itself issues
# no-replicated-database: the hook never reattaches tables of a `Replicated` database

# - a table with a projection reading the table itself through a scalar subquery is not reattached;
# - a query limiting its memory below the default does not reattach a persistent `Set` table;
# Queries that fail on an access check made only after the hook must not reattach any of their tables:
# - a read of a table with a stored `IN` over a `Set` table the caller cannot read, joined with an
#   otherwise eligible table;
# - a read of a system table requiring a privilege of its own besides `SELECT`;
# - an insert into a cluster-mode `QueryRunner` table without the `REMOTE` source grant.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

USER_NAME="user_reattach_review_7_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${USER_NAME};
    DROP TABLE IF EXISTS t_in;
    DROP TABLE IF EXISTS secret;
    DROP TABLE IF EXISTS runner;
    DROP TABLE IF EXISTS src;
    DROP TABLE IF EXISTS p;
    DROP TABLE IF EXISTS s;
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1);
    CREATE TABLE secret (k UInt64) ENGINE = Set;
    INSERT INTO secret VALUES (1);
    CREATE TABLE t_in (k UInt64, v UInt8 ALIAS k IN ${CLICKHOUSE_DATABASE}.secret) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t_in VALUES (1);
    CREATE TABLE runner (query String, database String, settings Map(String, String)) ENGINE = QueryRunner
        SETTINGS cluster = 'test_shard_localhost', mode = 'synchronous';
    CREATE TABLE p (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO p VALUES (1);
    ALTER TABLE p ADD PROJECTION pr (SELECT x + (SELECT count() FROM ${CLICKHOUSE_DATABASE}.p) AS z ORDER BY z);
    CREATE TABLE s (k UInt64) ENGINE = Set;
    INSERT INTO s SELECT number FROM numbers(1000000);
    CREATE USER ${USER_NAME} IDENTIFIED WITH no_password;
    GRANT SELECT, INSERT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.src TO ${USER_NAME};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_in TO ${USER_NAME};
    GRANT SELECT ON system.jemalloc_bins TO ${USER_NAME};
    GRANT INSERT ON ${CLICKHOUSE_DATABASE}.runner TO ${USER_NAME};
    GRANT TABLE ENGINE ON MergeTree TO ${USER_NAME};
"

# Run the query as the restricted user; it must fail with the given error and must not detach any table.
function check_user_fails_without_detach()
{
    local output status
    output=$(${MY_CLICKHOUSE_CLIENT} --user "${USER_NAME}" --enable_parallel_replicas=0 --reattach_tables_before_query_execution=1 --query "$1" 2>&1)
    status=$?
    if [ "$status" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif ! echo "$output" | grep -q "$2"; then
        echo "FAIL (unexpected error: $output)"
    elif echo "$output" | grep -q "DETACH TABLE ${CLICKHOUSE_DATABASE}\."; then
        echo "FAIL (a table was detached: $output)"
    else
        echo "OK"
    fi
}

# A control: the same user can make the hook reattach the table.
function check_user_detaches()
{
    local output status
    output=$(${MY_CLICKHOUSE_CLIENT} --user "${USER_NAME}" --enable_parallel_replicas=0 --reattach_tables_before_query_execution=1 --query "$1" 2>&1)
    status=$?
    if [ "$status" -ne 0 ]; then
        echo "FAIL (client error: $output)"
    elif echo "$output" | grep -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.$2"; then
        echo "OK"
    else
        echo "FAIL ($2 was not detached)"
    fi
}

# The hook skips a table whose exclusive lock it cannot take within a short probe, and on a loaded runner
# a background task of a fresh table can hold its lock for that long. Retry the positive control until the
# hook gets the table idle.
function retry_until_ok()
{
    local result
    for _ in {1..10}
    do
        result=$("$@")
        [ "$result" = "OK" ] && break
    done
    echo "$result"
}

echo "control"
retry_until_ok check_user_detaches "SELECT x FROM src" src

echo "stored IN in a join"
check_user_fails_without_detach "SELECT src.x, a.v FROM src CROSS JOIN t_in AS a" "ACCESS_DENIED"

echo "system table with its own privilege in a join"
# Builds without jemalloc do not have the table at all.
check_user_fails_without_detach "SELECT src.x, b.size FROM src CROSS JOIN system.jemalloc_bins AS b" "ACCESS_DENIED\\|UNKNOWN_TABLE"

echo "insert into a cluster-mode QueryRunner"
check_user_fails_without_detach "INSERT INTO runner (query) SELECT 'SELECT 1' FROM src SETTINGS async_insert = 0" "ACCESS_DENIED"

echo "projection reading the table itself"
check_if_not_detached "SELECT x FROM p SETTINGS optimize_use_projections = 0" p
${CLICKHOUSE_CLIENT} -q "SELECT x FROM p"

echo "lowered memory limit"
check_if_not_detached "SELECT 1 IN s SETTINGS max_memory_usage = 10000000, max_untracked_memory = 1" s
${CLICKHOUSE_CLIENT} -q "SELECT 1 IN s, 999999 IN s"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE t_in;
    DROP TABLE secret;
    DROP TABLE runner;
    DROP TABLE src;
    DROP TABLE p;
    DROP TABLE s;
    DROP USER ${USER_NAME};
"
