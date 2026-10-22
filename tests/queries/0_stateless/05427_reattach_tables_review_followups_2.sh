#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test checks the `DETACH`/`ATTACH` the hook itself issues
# no-replicated-database: the hook never reattaches tables of a `Replicated` database

# Cases the hook must handle although the tables are otherwise eligible:
# - a `File` table that has written several files, which `ATTACH` would forget;
# - a `MergeTree` key added with a query-level setting the internal `ATTACH` does not run with;
# - a loaded table of a database with `lazy_load_tables` that a running `INSERT` holds;
# - a query reading a view whose inner table the caller cannot read;
# - query text carried by settings (`additional_result_filter`, `additional_table_filters`);
# - a refreshable materialized view, whose `SELECT` is resolved in the view's own database.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

USER_NAME="user_reattach_review_2_${CLICKHOUSE_DATABASE}"
LAZY_DATABASE="${CLICKHOUSE_DATABASE}_lazy"
OTHER_DATABASE="${CLICKHOUSE_DATABASE}_other"

# The hook skips a table whose exclusive lock it cannot take within a short probe, and on a loaded runner
# a background task of a fresh table can hold its lock for that long. The positive controls are read-only,
# so retry them until the hook gets the table idle.
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

echo "multi-file File table"
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_file_multi;
    DROP TABLE IF EXISTS t_file_single;
    CREATE TABLE t_file_multi (x UInt64) ENGINE = File(Parquet);
    INSERT INTO t_file_multi VALUES (1);
    INSERT INTO t_file_multi SETTINGS engine_file_allow_create_multiple_files = 1 VALUES (2);
    CREATE TABLE t_file_single (x UInt64) ENGINE = File(Parquet);
    INSERT INTO t_file_single VALUES (1);
"
check_if_not_detached "SELECT count() FROM t_file_multi" t_file_multi
${CLICKHOUSE_CLIENT} -q "SELECT x FROM t_file_multi ORDER BY x"
# A control: a `File` table with a single file is reattached.
retry_until_ok check_if_detached "SELECT count() FROM t_file_single" t_file_single

echo "setting-dependent sorting key"
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_key_tz;
    CREATE TABLE t_key_tz (x UInt64) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t_key_tz VALUES (1);
    ALTER TABLE t_key_tz ADD COLUMN dt DateTime, ADD COLUMN tz String, MODIFY ORDER BY toTimezone(dt, tz)
        SETTINGS allow_nonconst_timezone_arguments = 1;
"
# The internal `ATTACH` runs with the settings of the default profile. The test configuration
# (`tests/config/users.d/nonconst_timezone.xml`) enables `allow_nonconst_timezone_arguments` there, and then
# the key is valid for the `ATTACH`, so the table may be reattached.
DEFAULT_ALLOWS_NONCONST_TIMEZONE=$(${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.settings_profile_elements
    WHERE profile_name = 'default' AND setting_name = 'allow_nonconst_timezone_arguments' AND value IN ('1', 'true')")
if [ "$DEFAULT_ALLOWS_NONCONST_TIMEZONE" -eq 0 ]; then
    check_if_not_detached "SELECT count() FROM t_key_tz SETTINGS allow_nonconst_timezone_arguments = 0" t_key_tz
else
    retry_until_ok check_if_detached "SELECT count() FROM t_key_tz SETTINGS allow_nonconst_timezone_arguments = 0" t_key_tz
fi
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_key_tz"

echo "busy lazy table"
${CLICKHOUSE_CLIENT} -q "
    DROP DATABASE IF EXISTS ${LAZY_DATABASE};
    CREATE DATABASE ${LAZY_DATABASE} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE ${LAZY_DATABASE}.t (a UInt64) ENGINE = MergeTree ORDER BY a;
    DETACH DATABASE ${LAZY_DATABASE};
    ATTACH DATABASE ${LAZY_DATABASE};
"
# Load the table, so that the catalog holds a loaded `StorageTableProxy`.
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM ${LAZY_DATABASE}.t FORMAT Null"

BUSY_QUERY_ID="reattach_lazy_busy_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query_id "$BUSY_QUERY_ID" --function_sleep_max_microseconds_per_block 60000000 \
    -q "INSERT INTO ${LAZY_DATABASE}.t SELECT sleep(1) FROM numbers(30) SETTINGS max_block_size = 1" > /dev/null 2>&1 &
busy_pid=$!

busy_running=0
for _ in {0..600}
do
    if [[ "$(${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.processes WHERE query_id = '$BUSY_QUERY_ID'")" -gt 0 ]]
    then
        busy_running=1
        break
    fi
    sleep 0.1
done
if [ "$busy_running" -ne 1 ]
then
    echo "FAIL (the concurrent INSERT did not show up in system.processes)"
fi

check_if_detached_impl "SELECT count() FROM ${LAZY_DATABASE}.t FORMAT Null"
if [ "$REATTACH_STATUS" -ne 0 ]; then
    echo "FAIL (client error: $REATTACH_OUTPUT)"
elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE ${LAZY_DATABASE}.t"; then
    echo "FAIL (the busy lazy table was detached)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "KILL QUERY WHERE query_id = '$BUSY_QUERY_ID' SYNC" > /dev/null 2>&1
wait "$busy_pid" 2>/dev/null
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${LAZY_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${USER_NAME};
    DROP VIEW IF EXISTS v_invoker;
    DROP TABLE IF EXISTS src;
    DROP TABLE IF EXISTS secret;
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1);
    CREATE TABLE secret (y UInt64) ENGINE = MergeTree ORDER BY y;
    INSERT INTO secret VALUES (1);
    CREATE VIEW v_invoker SQL SECURITY INVOKER AS SELECT y FROM secret;
    CREATE USER ${USER_NAME} IDENTIFIED WITH no_password;
    GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.src TO ${USER_NAME};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_invoker TO ${USER_NAME};
    GRANT TABLE ENGINE ON MergeTree TO ${USER_NAME};
"

# Run the query as the restricted user; it must fail with the given error and must not detach `src`.
function check_user_fails_without_detach()
{
    local output status
    output=$(${MY_CLICKHOUSE_CLIENT} --user "${USER_NAME}" --enable_parallel_replicas=0 --reattach_tables_before_query_execution=1 --query "$1" 2>&1)
    status=$?
    if [ "$status" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif ! echo "$output" | grep -q "$2"; then
        echo "FAIL (unexpected error: $output)"
    elif echo "$output" | grep -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.src"; then
        echo "FAIL (src was detached)"
    else
        echo "OK"
    fi
}

# A control: the same user can make the hook reattach `src`.
function check_user_detaches()
{
    local output status
    output=$(${MY_CLICKHOUSE_CLIENT} --user "${USER_NAME}" --enable_parallel_replicas=0 --reattach_tables_before_query_execution=1 --query "$1" 2>&1)
    status=$?
    if [ "$status" -ne 0 ]; then
        echo "FAIL (client error: $output)"
    elif echo "$output" | grep -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.src"; then
        echo "OK"
    else
        echo "FAIL (src was not detached)"
    fi
}

echo "control"
retry_until_ok check_user_detaches "SELECT count() FROM src"

echo "view with an inaccessible inner table"
check_user_fails_without_detach "SELECT src.x FROM src CROSS JOIN v_invoker" "ACCESS_DENIED"

echo "query text in settings"
check_user_fails_without_detach "SELECT x FROM src SETTINGS additional_result_filter = 'empty(file(''anything'', ''''))'" "ACCESS_DENIED"
check_user_fails_without_detach "SELECT x FROM src SETTINGS additional_table_filters = {'src': 'x IN (SELECT y FROM secret)'}" "ACCESS_DENIED"

echo "refreshable materialized view"
${CLICKHOUSE_CLIENT} -q "
    DROP DATABASE IF EXISTS ${OTHER_DATABASE};
    CREATE DATABASE ${OTHER_DATABASE};
    CREATE TABLE ${OTHER_DATABASE}.src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO ${OTHER_DATABASE}.src VALUES (2);
"
output=$(${MY_CLICKHOUSE_CLIENT} --reattach_tables_before_query_execution=1 --query "
    CREATE MATERIALIZED VIEW ${OTHER_DATABASE}.mv REFRESH EVERY 1 YEAR APPEND
    ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM src" 2>&1)
status=$?
if [ "$status" -ne 0 ]; then
    echo "FAIL (client error: $output)"
elif echo "$output" | grep -q "DETACH TABLE ${CLICKHOUSE_DATABASE}.src"; then
    echo "FAIL (src of the session database was detached)"
else
    echo "OK"
fi

${CLICKHOUSE_CLIENT} -q "
    DROP DATABASE IF EXISTS ${OTHER_DATABASE};
    DROP USER IF EXISTS ${USER_NAME};
    DROP VIEW IF EXISTS v_invoker;
    DROP TABLE IF EXISTS src;
    DROP TABLE IF EXISTS secret;
    DROP TABLE IF EXISTS t_file_multi;
    DROP TABLE IF EXISTS t_file_single;
    DROP TABLE IF EXISTS t_key_tz;
"
