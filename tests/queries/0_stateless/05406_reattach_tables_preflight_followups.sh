#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test checks the `DETACH`/`ATTACH` the hook itself issues
# no-replicated-database: the hook never reattaches tables of a `Replicated` database

# Queries the hook must leave alone although their tables are otherwise eligible:
# - a subquery that may be answered from the query result cache;
# - a statement that only stores a query (`CREATE HANDLER`);
# - statements rejected later on privileges or row policies that a plain table grant does not cover;
# - a mutation on a table that is still a lazy stand-in, by a user who may not run the mutation.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

USER_NAME="user_reattach_followups_${CLICKHOUSE_DATABASE}"
LAZY_DATABASE="${CLICKHOUSE_DATABASE}_lazy"
HANDLER_NAME="h_reattach_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${USER_NAME};
    DROP TABLE IF EXISTS src;
    CREATE TABLE src (k UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY k;
    INSERT INTO src VALUES (1, [1, 1]);
"

# Run the query as the restricted user; it must fail with the given error and must not detach the table.
# Parallel replicas are disabled: with them, the `JOIN` against the `Join` table under a row policy is
# not rejected, which is not what this check is about.
function check_user_fails_without_detach()
{
    local output status
    output=$(${MY_CLICKHOUSE_CLIENT} --user "${USER_NAME}" --enable_parallel_replicas=0 --reattach_tables_before_query_execution=1 --query "$1" 2>&1)
    status=$?
    if [ "$status" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif ! echo "$output" | grep -q "$3"; then
        echo "FAIL (unexpected error: $output)"
    elif echo "$output" | grep -q "DETACH TABLE $2"; then
        echo "FAIL ($2 was detached)"
    else
        echo "OK"
    fi
}

# A control: the same user can make the hook reattach the table.
function check_user_detaches()
{
    local output status
    output=$(${MY_CLICKHOUSE_CLIENT} --user "${USER_NAME}" --reattach_tables_before_query_execution=1 --query "$1" 2>&1)
    status=$?
    if [ "$status" -ne 0 ]; then
        echo "FAIL (client error: $output)"
    elif echo "$output" | grep -q "DETACH TABLE $2"; then
        echo "OK"
    else
        echo "FAIL ($2 was not detached)"
    fi
}

function recreate_user()
{
    ${CLICKHOUSE_CLIENT} -q "
        DROP USER IF EXISTS ${USER_NAME};
        CREATE USER ${USER_NAME} IDENTIFIED WITH no_password;
        GRANT ALL ON ${CLICKHOUSE_DATABASE}.src TO ${USER_NAME};
        GRANT TABLE ENGINE ON MergeTree TO ${USER_NAME};
    "
}

echo "query cache"
# A subquery opting in to the cache may be served from it without reading `src`.
check_if_not_detached "SELECT * FROM (SELECT k FROM src SETTINGS use_query_cache = 1) SETTINGS use_query_cache = 0" src

echo "handler"
check_if_not_detached "CREATE HANDLER ${HANDLER_NAME} URL '/${HANDLER_NAME}' AS SELECT k FROM src" src
${CLICKHOUSE_CLIENT} -q "DROP HANDLER IF EXISTS ${HANDLER_NAME}"
recreate_user
check_user_fails_without_detach "CREATE HANDLER ${HANDLER_NAME} URL '/${HANDLER_NAME}' AS SELECT k FROM src" "${CLICKHOUSE_DATABASE}.src" "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} -q "DROP HANDLER IF EXISTS ${HANDLER_NAME}"

echo "create destination"
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS dst;
    DROP TABLE IF EXISTS plain;
    CREATE TABLE plain (k UInt64) ENGINE = MergeTree ORDER BY k;
    GRANT CREATE TABLE ON ${CLICKHOUSE_DATABASE}.dst TO ${USER_NAME};
    GRANT CREATE VIEW, DROP VIEW ON ${CLICKHOUSE_DATABASE}.plain TO ${USER_NAME};
"
check_user_detaches "SELECT count() FROM src" "${CLICKHOUSE_DATABASE}.src"
# Populating `dst` needs `INSERT` on it.
check_user_fails_without_detach "CREATE TABLE dst ENGINE = MergeTree ORDER BY tuple() AS SELECT k FROM src" "${CLICKHOUSE_DATABASE}.src" "ACCESS_DENIED"
# Replacing the plain table `plain` with a view needs `DROP TABLE` on it.
check_user_fails_without_detach "CREATE OR REPLACE VIEW plain AS SELECT k FROM src" "${CLICKHOUSE_DATABASE}.src" "ACCESS_DENIED"

echo "dictionary functions"
${CLICKHOUSE_CLIENT} -q "
    DROP DICTIONARY IF EXISTS d_centroids;
    DROP TABLE IF EXISTS centroids;
    CREATE TABLE centroids (cid UInt64, vec Array(Float32)) ENGINE = MergeTree ORDER BY cid;
    INSERT INTO centroids VALUES (1, [1, 1]), (2, [5, 5]);
    CREATE DICTIONARY d_centroids (cid UInt64, vec Array(Float32)) PRIMARY KEY cid
        SOURCE(CLICKHOUSE(TABLE 'centroids' DB '${CLICKHOUSE_DATABASE}')) LAYOUT(FLAT(MAX_ARRAY_SIZE 1000)) LIFETIME(0);
    SYSTEM RELOAD DICTIONARY d_centroids;
"
check_user_fails_without_detach "SELECT assignCentroid(vec, 'd_centroids') FROM src" "${CLICKHOUSE_DATABASE}.src" "ACCESS_DENIED"
# The inline form names no dictionary.
check_user_detaches "SELECT assignCentroid(vec, [[1.0, 1.0], [5.0, 5.0]]) FROM src" "${CLICKHOUSE_DATABASE}.src"

echo "row policy"
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS j;
    DROP TABLE IF EXISTS s;
    CREATE TABLE j (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
    INSERT INTO j VALUES (1, 10);
    CREATE TABLE s (k UInt64) ENGINE = Set;
    INSERT INTO s VALUES (1);
    GRANT ALL ON ${CLICKHOUSE_DATABASE}.j TO ${USER_NAME};
    GRANT ALL ON ${CLICKHOUSE_DATABASE}.s TO ${USER_NAME};
    GRANT TABLE ENGINE ON Join TO ${USER_NAME};
    GRANT TABLE ENGINE ON Set TO ${USER_NAME};
"
check_user_detaches "SELECT joinGet('j', 'v', toUInt64(1))" "${CLICKHOUSE_DATABASE}.j"
${CLICKHOUSE_CLIENT} -q "
    DROP ROW POLICY IF EXISTS p_reattach_j ON ${CLICKHOUSE_DATABASE}.j;
    DROP ROW POLICY IF EXISTS p_reattach_s ON ${CLICKHOUSE_DATABASE}.s;
    CREATE ROW POLICY p_reattach_j ON ${CLICKHOUSE_DATABASE}.j USING k = 1 TO ${USER_NAME};
    CREATE ROW POLICY p_reattach_s ON ${CLICKHOUSE_DATABASE}.s USING k = 1 TO ${USER_NAME};
"
check_user_fails_without_detach "SELECT joinGet('j', 'v', toUInt64(1))" "${CLICKHOUSE_DATABASE}.j" "ACCESS_DENIED"
check_user_fails_without_detach "SELECT * FROM src ANY LEFT JOIN j USING k" "${CLICKHOUSE_DATABASE}.src" "ACCESS_DENIED"
check_user_fails_without_detach "SELECT count() FROM src WHERE k IN s" "${CLICKHOUSE_DATABASE}.src" "ACCESS_DENIED"

echo "lazy mutation target"
${CLICKHOUSE_CLIENT} -q "
    DROP DATABASE IF EXISTS ${LAZY_DATABASE};
    CREATE DATABASE ${LAZY_DATABASE} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE ${LAZY_DATABASE}.t (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO ${LAZY_DATABASE}.t VALUES (1);
    DETACH DATABASE ${LAZY_DATABASE};
    ATTACH DATABASE ${LAZY_DATABASE};
    GRANT SELECT, DROP TABLE, CREATE TABLE ON ${LAZY_DATABASE}.t TO ${USER_NAME};
"
check_user_fails_without_detach "DELETE FROM ${LAZY_DATABASE}.t WHERE 1" "${LAZY_DATABASE}.t" "ACCESS_DENIED"
# The table is still the stand-in: the rejected statement did not load it.
${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${LAZY_DATABASE}' AND name = 't'"

${CLICKHOUSE_CLIENT} -q "
    DROP DATABASE IF EXISTS ${LAZY_DATABASE};
    DROP ROW POLICY IF EXISTS p_reattach_j ON ${CLICKHOUSE_DATABASE}.j;
    DROP ROW POLICY IF EXISTS p_reattach_s ON ${CLICKHOUSE_DATABASE}.s;
    DROP USER IF EXISTS ${USER_NAME};
    DROP DICTIONARY IF EXISTS d_centroids;
    DROP TABLE IF EXISTS centroids;
    DROP TABLE IF EXISTS j;
    DROP TABLE IF EXISTS s;
    DROP TABLE IF EXISTS dst;
    DROP TABLE IF EXISTS plain;
    DROP TABLE IF EXISTS src;
"
