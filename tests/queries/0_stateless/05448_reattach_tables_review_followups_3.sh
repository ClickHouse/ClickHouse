#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test checks the `DETACH`/`ATTACH` the hook itself issues
# no-replicated-database: the hook never reattaches tables of a `Replicated` database

# Cases the hook must handle although the tables are otherwise eligible:
# - `WITH` definitions the query never references;
# - a read of a storage that reads other tables (`Buffer`, `Merge`) the caller cannot read;
# - a stored constraint that the internal `ATTACH` cannot analyze under its settings;
# - an unqualified `joinGet` in the `SELECT` of a refreshable materialized view.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

USER_NAME="user_reattach_review_3_${CLICKHOUSE_DATABASE}"
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

${CLICKHOUSE_CLIENT} -q "
    DROP USER IF EXISTS ${USER_NAME};
    DROP TABLE IF EXISTS buf;
    DROP TABLE IF EXISTS m;
    DROP TABLE IF EXISTS src;
    DROP TABLE IF EXISTS secret;
    CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src VALUES (1);
    CREATE TABLE secret (y UInt64) ENGINE = MergeTree ORDER BY y;
    INSERT INTO secret VALUES (1);
    CREATE TABLE buf (y UInt64) ENGINE = Buffer(${CLICKHOUSE_DATABASE}, secret, 1, 1, 10, 1, 10, 1, 10);
    CREATE TABLE m (y UInt64) ENGINE = Merge(${CLICKHOUSE_DATABASE}, '^secret$');
    CREATE USER ${USER_NAME} IDENTIFIED WITH no_password;
    GRANT SELECT, DROP TABLE, CREATE TABLE ON ${CLICKHOUSE_DATABASE}.src TO ${USER_NAME};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.buf TO ${USER_NAME};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.m TO ${USER_NAME};
    -- The Merge engine skips the tables the user cannot see, and checks SELECT on the visible ones.
    GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.secret TO ${USER_NAME};
    GRANT TABLE ENGINE ON MergeTree TO ${USER_NAME};
"

echo "unused WITH definitions"
check_if_not_detached "WITH unused AS (SELECT x FROM src) SELECT 1" src
check_if_not_detached "WITH (SELECT max(x) FROM src) AS unused SELECT 1" src
# A control: a referenced definition is collected.
retry_until_ok check_if_detached "WITH used AS (SELECT x FROM src) SELECT count() FROM used" src

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

echo "storages reading other tables"
retry_until_ok check_user_detaches "SELECT count() FROM src"
check_user_fails_without_detach "SELECT src.x FROM src CROSS JOIN buf" "ACCESS_DENIED"
check_user_fails_without_detach "SELECT src.x FROM src CROSS JOIN m" "ACCESS_DENIED"

echo "setting-dependent constraint"
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_constraint_tz;
    CREATE TABLE t_constraint_tz
    (
        x UInt64,
        CONSTRAINT c CHECK x < (SELECT toUInt64(toTimezone(toDateTime(number), materialize('UTC'))) + 1000 FROM numbers(1))
    )
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS allow_nonconst_timezone_arguments = 1;
"
# The internal `ATTACH` runs with the settings of the default profile. The test configuration
# (`tests/config/users.d/nonconst_timezone.xml`) enables `allow_nonconst_timezone_arguments` there, and then
# the constraint is valid for the `ATTACH`, so the table may be reattached.
DEFAULT_ALLOWS_NONCONST_TIMEZONE=$(${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.settings_profile_elements
    WHERE profile_name = 'default' AND setting_name = 'allow_nonconst_timezone_arguments' AND value IN ('1', 'true')")
if [ "$DEFAULT_ALLOWS_NONCONST_TIMEZONE" -eq 0 ]; then
    check_if_not_detached "SELECT count() FROM t_constraint_tz SETTINGS allow_nonconst_timezone_arguments = 0" t_constraint_tz
else
    retry_until_ok check_if_detached "SELECT count() FROM t_constraint_tz SETTINGS allow_nonconst_timezone_arguments = 0" t_constraint_tz
fi
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM t_constraint_tz"

echo "joinGet in a refreshable materialized view"
${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS j;
    DROP DATABASE IF EXISTS ${OTHER_DATABASE};
    CREATE DATABASE ${OTHER_DATABASE};
    CREATE TABLE j (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
    INSERT INTO j VALUES (1, 10);
    CREATE TABLE ${OTHER_DATABASE}.j (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
    INSERT INTO ${OTHER_DATABASE}.j VALUES (1, 20);
"
# A control: `j` of the session database is reattached for a plain `joinGet`.
retry_until_ok check_if_detached "SELECT joinGet('j', 'v', toUInt64(1))" j
check_if_not_detached "
    CREATE MATERIALIZED VIEW ${OTHER_DATABASE}.mv REFRESH EVERY 1 YEAR APPEND
    ENGINE = MergeTree ORDER BY tuple() AS SELECT joinGet('j', 'v', toUInt64(1)) AS v" j

${CLICKHOUSE_CLIENT} -q "
    DROP DATABASE IF EXISTS ${OTHER_DATABASE};
    DROP USER IF EXISTS ${USER_NAME};
    DROP TABLE IF EXISTS buf;
    DROP TABLE IF EXISTS m;
    DROP TABLE IF EXISTS src;
    DROP TABLE IF EXISTS secret;
    DROP TABLE IF EXISTS t_constraint_tz;
    DROP TABLE IF EXISTS j;
"
